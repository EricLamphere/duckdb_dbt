{{ config(materialized='ephemeral') }}
with
    products as (
        select
            *
        from
            {{ ref('stg_products') }}
    )
select
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    list_price_dollars,
    cost_dollars,
    supplier_id,
    is_active,
    round(list_price_dollars - cost_dollars, 2) as margin_dollars,
    case
        when list_price_dollars < 25 then 'low'
        when list_price_dollars < 100 then 'mid'
        else 'high'
    end as price_tier
from
    products