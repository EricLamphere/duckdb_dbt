with
    source as (
        select
            *
        from
            {{ source('raw', 'raw_sales') }}
    )
select
    order_id,
    customer_id,
    product_id,
    cast(order_ts as timestamp) as order_ts,
    quantity,
    {{ cents_to_dollars('unit_price_cents') }} as unit_price_dollars,
    {{ cents_to_dollars('discount_cents') }} as discount_dollars,
    quantity * {{ cents_to_dollars('unit_price_cents') }} as gross_amount_dollars,
    lower(channel) as channel,
    store_id,
    current_timestamp as loaded_at
from
    source