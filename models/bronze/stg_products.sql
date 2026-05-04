with
    source as (
        select
            *
        from
            {{ source('raw', 'raw_products') }}
    )
select
    product_id,
    sku,
    product_name,
    lower(category) as category,
    lower(subcategory) as subcategory,
    {{ cents_to_dollars('list_price_cents') }} as list_price_dollars,
    {{ cents_to_dollars('cost_cents') }} as cost_dollars,
    supplier_id,
    cast(is_active as boolean) as is_active
from
    source