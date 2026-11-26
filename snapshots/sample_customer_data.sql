{% snapshot sample_customer_data %}

{{
    config (
        unique_key = 'customer_id',
        strategy = 'timestamp',
        updated_at='load_timestamp'
    )
}}

select

    customer_id
    , first_name
    , last_name
    , load_timestamp

from {{ source('sample', 'customers_10000') }}

{% endsnapshot %}