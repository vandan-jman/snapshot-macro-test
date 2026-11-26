{% snapshot sample_customer_data %}

{{
    config (
        unique_key = 'customer_id',
        strategy = 'timestamp',
        updated_at = 'load_timestamp',
        pre_hook = "{{ handle_snapshot_changes('pre') }}",
        post_hook = "{{ handle_snapshot_changes('post') }}"
    )
}}

-- depends_on: {{ ref('snapshot_column_changes') }}

select

    customer_id
    , first_name
    , last_name
    , company           AS company_name
    , load_timestamp

from {{ source('sample', 'customers_10000') }}

{% endsnapshot %}