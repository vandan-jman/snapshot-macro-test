{#
  snapshot_changes_ledger.sql   (Databricks / Delta version)

  The ledger is a plain Delta table (NOT a seed) that records which registry rows have already been applied in THIS
  environment. A registry row is identified by (snapshot_name, change_id, registry_sequence):
    change_id         = hash of the row content (see _sc_row_id in handle_snapshot_changes.sql)
    registry_sequence = the row's `sequence` value, so the SAME operation can be repeated later by giving the
                        repeat a NEW sequence number.
  Ledger rows written before registry_sequence existed have NULL there; they are treated as belonging to the
  first (lowest-sequence) registry row with the same content, and are stamped with its sequence on first sight.
#}


{# Escape a value as a Databricks string literal: backslash first, then the single quote (Spark SQL does not
   treat '' as an escaped quote, it concatenates adjacent literals). #}
{% macro _sc_sql_str(value) %}
  {{ return("'" ~ ((value if value is not none else '') | string | replace('\\', '\\\\') | replace("'", "\\'")) ~ "'") }}
{% endmacro %}


{% macro _sc_ledger_rel() %}
  {{ return(api.Relation.create(database=target.database, schema=target.schema, identifier='snapshot_column_changes_applied')) }}
{% endmacro %}


{# Create the ledger once, from on-run-start, so parallel snapshot threads never race to create it.
   An existing ledger from an earlier iteration gets any missing column added. #}
{% macro create_snapshot_changes_ledger() %}
  {% if execute %}
    {% set rel = _sc_ledger_rel() %}
    {% set existing = adapter.get_relation(database=rel.database, schema=rel.schema, identifier=rel.identifier) %}
    {% if existing is none %}
      {% do run_query("create table " ~ rel ~ " (snapshot_name string, change_id string, action string, column_name string, new_column_name string, registry_sequence int, outcome string, applied_at timestamp) using delta") %}
      {{ log('[snapshot_changes] created ledger table ' ~ rel, info=True) }}
    {% else %}
      {% set have = [] %}
      {% for c in adapter.get_columns_in_relation(existing) %}
        {% do have.append(c.name | lower) %}
      {% endfor %}
      {% if 'new_column_name' not in have %}
        {% do run_query("alter table " ~ rel ~ " add columns (new_column_name string)") %}
        {{ log('[snapshot_changes] added new_column_name to ledger table ' ~ rel, info=True) }}
      {% endif %}
      {% if 'registry_sequence' not in have %}
        {% do run_query("alter table " ~ rel ~ " add columns (registry_sequence int)") %}
        {{ log('[snapshot_changes] added registry_sequence to ledger table ' ~ rel, info=True) }}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}


{# True if this registry row (content id + sequence) has already been applied in this environment.
   is_first = this row has the lowest sequence among registry rows with the same content id. #}
{% macro _sc_is_applied(snapshot, change_id, registry_sequence, is_first) %}
  {% if change_id is none or not modules.re.match('^[A-Za-z0-9_-]+$', change_id) %}
    {{ exceptions.raise_compiler_error("[snapshot_changes] invalid or missing change_id: " ~ change_id) }}
  {% endif %}
  {% set seq = registry_sequence | int %}
  {% set q %}
    select
      sum(case when registry_sequence = {{ seq }} then 1 else 0 end) as exact_n,
      sum(case when registry_sequence is null then 1 else 0 end) as legacy_n
    from {{ _sc_ledger_rel() }}
    where snapshot_name = {{ _sc_sql_str(snapshot) }} and change_id = {{ _sc_sql_str(change_id) }}
  {% endset %}
  {% set res = run_query(q).rows[0] %}
  {% set exact_n = res['exact_n'] or 0 %}
  {% set legacy_n = res['legacy_n'] or 0 %}
  {% if exact_n > 0 %}
    {{ return(true) }}
  {% elif legacy_n > 0 and is_first %}
    {# legacy ledger row: bind it to this (first) registry row so a later repeat is treated as a new entry #}
    {% do run_query("update " ~ _sc_ledger_rel() ~ " set registry_sequence = " ~ seq ~ " where snapshot_name = " ~ _sc_sql_str(snapshot) ~ " and change_id = " ~ _sc_sql_str(change_id) ~ " and registry_sequence is null") %}
    {{ return(true) }}
  {% else %}
    {{ return(false) }}
  {% endif %}
{% endmacro %}


{# new_column_name is optional (renames only). registry_sequence is the registry row's sequence value. #}
{% macro _sc_record(snapshot, change_id, action, column_name, outcome, new_column_name=none, registry_sequence=none) %}
  {% set new_col = _sc_sql_str(new_column_name) if new_column_name else 'null' %}
  {% set seq_sql = (registry_sequence | int) | string if registry_sequence is not none else 'null' %}
  {% do run_query("insert into " ~ _sc_ledger_rel() ~ " (snapshot_name, change_id, action, column_name, new_column_name, registry_sequence, outcome, applied_at) values (" ~ _sc_sql_str(snapshot) ~ ", " ~ _sc_sql_str(change_id) ~ ", " ~ _sc_sql_str(action) ~ ", " ~ _sc_sql_str(column_name) ~ ", " ~ new_col ~ ", " ~ seq_sql ~ ", " ~ _sc_sql_str(outcome) ~ ", current_timestamp())") %}
{% endmacro %}