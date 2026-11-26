{#
  handle_snapshot_changes.sql   (Databricks / Delta version)
  
  One macro, two phases, driven by the seed `snapshot_column_changes`:
    handle_snapshot_changes('pre')   -> RENAME rows   (run BEFORE the snapshot)
    handle_snapshot_changes('post')  -> ADD (backfill) and DROP rows (run AFTER the snapshot)

  A registry row is identified by its content hash plus its `sequence`, and is skipped when the per-environment
  ledger already holds that pair. To repeat an earlier operation, add a NEW row with a NEW, higher sequence.
  Never renumber or reuse the sequence of a row that has already run.

  fill_scope (add rows only; blank = current):
    current          current rows (dbt_valid_to IS NULL) are filled from the source. Closed rows stay NULL.
    all              current AND closed rows are filled from the source (only for values that never change).
    default:<value>  current rows are filled from the source; closed rows still NULL get the fixed <value>.

  Databricks specifics:
    - RENAME COLUMN and DROP COLUMN need Delta column mapping. The macro enables it on the snapshot table when it is
      missing (set var snapshot_changes_enable_column_mapping: false to forbid that and fail instead).
      Enabling column mapping upgrades the table protocol and cannot be turned off again without rewriting data.
    - A rename is metadata-only: no data is copied or rewritten.
    - The backfill uses MERGE INTO, because Databricks SQL has no UPDATE ... FROM ... JOIN.
#}


{% macro _sc_ident(name) %}
  {# Allow only plain identifiers, then backtick-quote for Databricks SQL #}
  {% if name is none or not modules.re.match('^[A-Za-z0-9_]+$', name) %}
    {{ exceptions.raise_compiler_error("[snapshot_changes] invalid identifier: " ~ name) }}
  {% endif %}
  {{ return('`' ~ name ~ '`') }}
{% endmacro %}


{% macro _sc_columns(rel) %}
  {# lower-case name -> actual name, so matching is case-insensitive but SQL uses the real spelling #}
  {% set cols = {} %}
  {% for c in adapter.get_columns_in_relation(rel) %}
    {% do cols.update({c.name | lower: c.name}) %}
  {% endfor %}
  {{ return(cols) }}
{% endmacro %}


{% macro _sc_ensure_column_mapping(rel, tag) %}
  {# RENAME COLUMN / DROP COLUMN only work on Delta tables with column mapping enabled (name or id mode). #}
  {% set modes = [] %}
  {% for p in run_query('SHOW TBLPROPERTIES ' ~ rel).rows %}
    {% if (p[0] | string | lower) == 'delta.columnmapping.mode' %}
      {% do modes.append(p[1] | string | lower) %}
    {% endif %}
  {% endfor %}
  {% if modes | length > 0 and modes[0] in ['name', 'id'] %}
    {{ return('') }}
  {% endif %}
  {% if not var('snapshot_changes_enable_column_mapping', true) %}
    {{ exceptions.raise_compiler_error(tag ~ "Delta column mapping is not enabled on " ~ rel ~ " and snapshot_changes_enable_column_mapping is false. Enable it manually: ALTER TABLE " ~ rel ~ " SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')") }}
  {% endif %}
  {% do run_query("ALTER TABLE " ~ rel ~ " SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')") %}
  {{ log(tag ~ 'ENABLED Delta column mapping (mode name) on ' ~ rel ~ '. This protocol upgrade is not reversible', info=True) }}
  {{ return('') }}
{% endmacro %}


{% macro _sc_row_id(r) %}
  {# Content hash of a registry row. confirm and sequence are NOT part of it (sequence is stored in the ledger
     next to it). fill_scope is part of it only when it is not the default, so existing ids stay unchanged. #}
  {% set fs = (r['fill_scope'] or '') | trim %}
  {% set fs_part = '' if (fs | lower) in ['', 'current'] else '|' ~ fs %}
  {{ return(local_md5(
       (r['action'] ~ '|' ~ r['column_name'] ~ '|' ~ (r['new_column_name'] or '') ~ '|' ~
        (r['source_name'] or '') ~ '|' ~ (r['source_table'] or '') ~ '|' ~
        (r['source_column'] or '') ~ '|' ~ (r['snapshot_key_columns'] or '') ~ '|' ~
        (r['source_key_columns'] or '') ~ fs_part) | lower
     )) }}
{% endmacro %}


{% macro _sc_warn_rename_drift(registry, snap_rel, tag) %}
  {# Post-hook check: a rename in the registry whose OLD and NEW column both exist means the snapshot SELECT is
     probably still producing the old name, so dbt keeps re-adding it. Warning only, nothing is changed. #}
  {% set ren_sql %}
    select column_name, new_column_name
    from {{ registry }}
    where lower(snapshot_name) = lower({{ _sc_sql_str(snap_rel.identifier) }})
      and lower(action) = 'rename'
  {% endset %}
  {% set cols = _sc_columns(snap_rel) %}
  {% for rr in run_query(ren_sql).rows %}
    {% set o = (rr['column_name'] or '') | lower %}
    {% set n = (rr['new_column_name'] or '') | lower %}
    {% if o in cols and n in cols %}
      {{ log(tag ~ 'WARNING both ' ~ cols[o] ~ ' and ' ~ cols[n] ~ ' exist although the registry renames ' ~ cols[o] ~ ' to ' ~ cols[n] ~ '. The snapshot SELECT probably still produces ' ~ cols[o] ~ ' (use ' ~ rr['new_column_name'] ~ ' there). Check the data, then drop the stray column', info=True) }}
    {% endif %}
  {% endfor %}
{% endmacro %}


{% macro handle_snapshot_changes(phase) %}

  {% if not execute %}
    {{ return('') }}
  {% endif %}

  {% set tag = '[snapshot_changes][' ~ phase ~ '][' ~ this.identifier ~ '] ' %}
  {% set actions = ['rename'] if phase == 'pre' else ['add', 'drop'] %}

  {# Registry must exist #}
  {% set registry = ref('snapshot_column_changes') %}
  {% set registry_rel = adapter.get_relation(database=registry.database, schema=registry.schema, identifier=registry.identifier) %}
  {% if registry_rel is none %}
    {{ log(tag ~ 'WARNING registry table not found, nothing done', info=True) }}
    {{ return('') }}
  {% endif %}

  {# Snapshot table must exist (the very first run has nothing to change) #}
  {% set snap_rel = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) %}
  {% if snap_rel is none %}
    {{ log(tag ~ 'snapshot table does not exist yet, skipping', info=True) }}
    {{ return('') }}
  {% endif %}

  {% set rows_sql %}
    select lower(action) as action, column_name, new_column_name,
           source_name, source_table, source_column,
           snapshot_key_columns, source_key_columns,
           fill_scope,
           `sequence`,
           lower(coalesce(confirm, 'false')) as confirm
    from {{ registry }}
    where lower(snapshot_name) = lower({{ _sc_sql_str(this.identifier) }})
      and lower(action) in ('{{ actions | join("','") }}')
    order by `sequence`
  {% endset %}
  {% set rows = run_query(rows_sql).rows %}

  {% if rows | length == 0 %}
    {% if phase == 'post' %}
      {% do _sc_warn_rename_drift(registry, snap_rel, tag) %}
    {% endif %}
    {{ log(tag ~ 'no registry entries, nothing to do', info=True) }}
    {{ return('') }}
  {% endif %}

  {# renames of this snapshot, used by 'add' rows whose column is renamed later in the registry #}
  {% set later_renames = [] %}
  {% if phase == 'post' %}
    {% set ren_sql %}
      select column_name, new_column_name, `sequence`
      from {{ registry }}
      where lower(snapshot_name) = lower({{ _sc_sql_str(this.identifier) }})
        and lower(action) = 'rename'
      order by `sequence`
    {% endset %}
    {% for rr in run_query(ren_sql).rows %}
      {% do later_renames.append({'old': rr['column_name'] | lower, 'new': rr['new_column_name'], 'seq': rr['sequence'] | int}) %}
    {% endfor %}
  {% endif %}

  {# first pass: every row needs a sequence; remember the lowest sequence per content id.
     rows are ordered by sequence, so the first one seen is the lowest. #}
  {% set first_seq = {} %}
  {% set seen = [] %}
  {% for r in rows %}
    {% if r['sequence'] is none %}
      {{ exceptions.raise_compiler_error(tag ~ "registry row for '" ~ r['column_name'] ~ "' has no sequence. A sequence is required, because it tells repeated operations apart") }}
    {% endif %}
    {% set rid = _sc_row_id(r) %}
    {% if rid not in first_seq %}
      {% do first_seq.update({rid: r['sequence'] | int}) %}
    {% endif %}
    {% set key = rid ~ '#' ~ (r['sequence'] | int) %}
    {% if key in seen %}
      {{ log(tag ~ 'WARNING two identical registry rows share sequence ' ~ r['sequence'] ~ ' (' ~ r['action'] ~ ' ' ~ r['column_name'] ~ '). They count as one entry. Give a repeat a new sequence', info=True) }}
    {% endif %}
    {% do seen.append(key) %}
  {% endfor %}

  {% for r in rows %}
    {# re-read the columns every time because earlier actions change them #}
    {% set cols = _sc_columns(snap_rel) %}
    {% set act = r['action'] %}
    {% set seq = r['sequence'] | int %}
    {% set fs = (r['fill_scope'] or '') | trim %}

    {# entry identity = content hash + sequence #}
    {% set cid = _sc_row_id(r) %}
    {% set done = _sc_is_applied(this.identifier, cid, seq, first_seq[cid] == seq) %}

    {% if done %}
      {{ log(tag ~ act ~ ' ' ~ r['column_name'] ~ ' (sequence ' ~ seq ~ ') already recorded as applied in the ledger, skipped', info=True) }}

    {# ---------------- RENAME ---------------- #}
    {% elif act == 'rename' %}
      {% set old = (r['column_name'] | lower) %}
      {% set new = (r['new_column_name'] | lower) %}
      {% set old_id = _sc_ident(r['column_name']) %}
      {% set new_id = _sc_ident(r['new_column_name']) %}

      {% if old in cols and new not in cols %}
        {% do _sc_ensure_column_mapping(this, tag) %}
        {% do run_query('ALTER TABLE ' ~ this ~ ' RENAME COLUMN ' ~ _sc_ident(cols[old]) ~ ' TO ' ~ new_id) %}
        {{ log(tag ~ 'RENAMED ' ~ cols[old] ~ ' -> ' ~ r['new_column_name'] ~ ' (metadata-only, Delta column mapping)', info=True) }}
        {% do _sc_record(this.identifier, cid, 'rename', r['column_name'], 'renamed', r['new_column_name'], seq) %}
      {% elif old not in cols and new in cols %}
        {{ log(tag ~ 'rename already applied (' ~ r['new_column_name'] ~ ' exists), skipped', info=True) }}
        {% do _sc_record(this.identifier, cid, 'rename', r['column_name'], 'already_applied', r['new_column_name'], seq) %}
      {% elif old in cols and new in cols %}
        {{ log(tag ~ 'WARNING both ' ~ r['column_name'] ~ ' and ' ~ r['new_column_name'] ~ ' exist, NOT renaming. Needs manual review', info=True) }}
      {% else %}
        {{ log(tag ~ 'WARNING neither ' ~ r['column_name'] ~ ' nor ' ~ r['new_column_name'] ~ ' exists, skipped', info=True) }}
      {% endif %}

    {# ---------------- ADD (backfill) ---------------- #}
    {% elif act == 'add' %}

      {# ---- fill_scope: current (default) | all | default:<value> ---- #}
      {% set scope_l = fs | lower %}
      {% set default_val = '' %}
      {% if scope_l in ['', 'current'] %}
        {% set scope = 'current' %}
      {% elif scope_l == 'all' %}
        {% set scope = 'all' %}
      {% elif scope_l.startswith('default:') %}
        {% set scope = 'default' %}
        {% set default_val = fs[8:] %}
        {% if (default_val | trim) == '' %}
          {{ exceptions.raise_compiler_error(tag ~ "fill_scope for '" ~ r['column_name'] ~ "' is 'default:' but has no value after the colon") }}
        {% endif %}
      {% else %}
        {{ exceptions.raise_compiler_error(tag ~ "invalid fill_scope '" ~ fs ~ "' for '" ~ r['column_name'] ~ "'. Allowed: current, all, default:<value>") }}
      {% endif %}

      {# ---- find the column; if it is renamed later in the registry, follow the rename chain ---- #}
      {% set tgt = (r['column_name'] | lower) %}
      {% if tgt not in cols and later_renames | length > 0 %}
        {% set holder = [tgt] %}
        {% for rn in later_renames %}
          {% if rn['seq'] > seq and rn['old'] == holder[-1] %}
            {% do holder.append(rn['new'] | lower) %}
          {% endif %}
        {% endfor %}
        {% if holder[-1] in cols %}
          {{ log(tag ~ r['column_name'] ~ ' is renamed later in the registry, backfilling its current name ' ~ cols[holder[-1]], info=True) }}
          {% set tgt = holder[-1] %}
        {% endif %}
      {% endif %}
      {% if tgt not in cols %}
        {{ log(tag ~ 'WARNING column ' ~ r['column_name'] ~ ' not in snapshot table. Is it in the snapshot SELECT?', info=True) }}
      {% else %}
        {% if not r['source_table'] or not r['source_column'] or not r['snapshot_key_columns'] %}
          {{ exceptions.raise_compiler_error(tag ~ "add entry for '" ~ r['column_name'] ~ "' needs source_table, source_column and snapshot_key_columns") }}
        {% endif %}

        {% if r['source_name'] %}
          {% set src_rel = source(r['source_name'], r['source_table']) %}
        {% else %}
          {% set src_rel = ref(r['source_table']) %}
        {% endif %}

        {% set src_cols = _sc_columns(src_rel) %}
        {% set snap_keys = r['snapshot_key_columns'].split('|') %}
        {% set src_keys = (r['source_key_columns'] if r['source_key_columns'] else r['snapshot_key_columns']).split('|') %}
        {% set problems = [] %}
        {% set join_parts = [] %}

        {% if snap_keys | length != src_keys | length %}
          {% do problems.append('snapshot_key_columns and source_key_columns have a different number of columns') %}
        {% else %}
          {% for i in range(snap_keys | length) %}
            {% set ks = snap_keys[i] | trim | lower %}
            {% set kr = src_keys[i] | trim | lower %}
            {% if ks not in cols %}
              {% do problems.append("snapshot key '" ~ snap_keys[i] ~ "' not found in snapshot " ~ this) %}
            {% endif %}
            {% if kr not in src_cols %}
              {% do problems.append("source key '" ~ src_keys[i] ~ "' not found in source " ~ src_rel) %}
            {% endif %}
            {% if ks in cols and kr in src_cols %}
              {% do join_parts.append('snap.' ~ _sc_ident(cols[ks]) ~ ' = src.' ~ _sc_ident(src_cols[kr])) %}
            {% endif %}
          {% endfor %}
        {% endif %}

        {% set sc_l = r['source_column'] | lower %}
        {% if sc_l not in src_cols %}
          {% do problems.append("source_column '" ~ r['source_column'] ~ "' not found in " ~ src_rel) %}
        {% endif %}

        {% if problems | length > 0 %}
          {{ exceptions.raise_compiler_error(tag ~ (problems | join(' | ')) ~ ' || source columns: ' ~ (src_cols.values() | list | join(', ')) ~ ' || snapshot columns: ' ~ (cols.values() | list | join(', '))) }}
        {% endif %}

        {% set t_col = _sc_ident(cols[tgt]) %}
        {% set s_col = _sc_ident(src_cols[sc_l]) %}
        {% set join_sql = join_parts | join(' and ') %}

        {# scope 'all' also touches closed rows; 'current' and 'default' copy from the source into current rows only #}
        {% set valid_filter = '' if scope == 'all' else 'and snap.dbt_valid_to is null' %}

        {# ---- step 1: copy from the source ---- #}
        {% set count_sql %}
          select count(*) as n
          from {{ this }} as snap
          inner join {{ src_rel }} as src on {{ join_sql }}
          where snap.{{ t_col }} is null
            {{ valid_filter }}
            and src.{{ s_col }} is not null
        {% endset %}
        {{ log(tag ~ 'count SQL: ' ~ count_sql, info=True) }}
        {% set n = run_query(count_sql).columns[0].values()[0] %}

        {% if n > 0 %}
          {# Databricks has no UPDATE ... FROM ... JOIN, so the copy is a MERGE. If the source holds several rows for one
             snapshot row, Delta stops the MERGE with an error instead of picking one. #}
          {% set upd_sql %}
            merge into {{ this }} as snap
            using {{ src_rel }} as src
            on {{ join_sql }}
            when matched
              and snap.{{ t_col }} is null
              {{ valid_filter }}
              and src.{{ s_col }} is not null
            then update set {{ t_col }} = src.{{ s_col }}
          {% endset %}
          {{ log(tag ~ 'merge SQL: ' ~ upd_sql, info=True) }}
          {% do run_query(upd_sql) %}
          {{ log(tag ~ 'BACKFILLED ' ~ n ~ ' rows in ' ~ cols[tgt] ~ ' from source (scope: ' ~ scope ~ ')', info=True) }}
        {% endif %}

        {# ---- step 2 (default scope only): fixed value for closed rows ---- #}
        {% set n_closed = 0 %}
        {% if scope == 'default' %}
          {% set closed_count_sql %}
            select count(*) as n
            from {{ this }} as snap
            where snap.{{ t_col }} is null
              and snap.dbt_valid_to is not null
          {% endset %}
          {% set n_closed = run_query(closed_count_sql).columns[0].values()[0] %}
          {% if n_closed > 0 %}
            {% set closed_upd_sql %}
              update {{ this }}
              set {{ t_col }} = {{ _sc_sql_str(default_val) }}
              where {{ t_col }} is null
                and dbt_valid_to is not null
            {% endset %}
            {{ log(tag ~ 'closed-row SQL: ' ~ closed_upd_sql, info=True) }}
            {% do run_query(closed_upd_sql) %}
            {{ log(tag ~ 'DEFAULTED ' ~ n_closed ~ ' closed rows in ' ~ cols[tgt] ~ " to '" ~ default_val ~ "'", info=True) }}
          {% endif %}
        {% endif %}

        {% if n > 0 or n_closed > 0 %}
          {% do _sc_record(this.identifier, cid, 'add', r['column_name'], 'backfilled_' ~ scope, none, seq) %}
        {% else %}
          {# deliberately NOT recorded: 0 rows can mean a key mismatch, so the entry stays live #}
          {{ log(tag ~ 'nothing to backfill for ' ~ cols[tgt] ~ ' (scope: ' ~ scope ~ ')', info=True) }}
        {% endif %}
      {% endif %}

    {# ---------------- DROP ---------------- #}
    {% elif act == 'drop' %}
      {% set tgt = (r['column_name'] | lower) %}
      {% if tgt not in cols %}
        {{ log(tag ~ 'drop already applied (' ~ r['column_name'] ~ ' not found), skipped', info=True) }}
        {% do _sc_record(this.identifier, cid, 'drop', r['column_name'], 'already_applied', none, seq) %}
      {% elif r['confirm'] != 'true' %}
        {{ log(tag ~ 'WARNING drop of ' ~ r['column_name'] ~ ' requested but confirm != true, NOT dropped', info=True) }}
      {% else %}
        {% do _sc_ensure_column_mapping(this, tag) %}
        {% do run_query('ALTER TABLE ' ~ this ~ ' DROP COLUMN ' ~ _sc_ident(cols[tgt])) %}
        {{ log(tag ~ 'DROPPED ' ~ cols[tgt], info=True) }}
        {% do _sc_record(this.identifier, cid, 'drop', r['column_name'], 'dropped', none, seq) %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {% if phase == 'post' %}
    {% do _sc_warn_rename_drift(registry, snap_rel, tag) %}
  {% endif %}

  {{ return('') }}
{% endmacro %}