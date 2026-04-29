{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

with sales as (
    select * from {{ ref('stg_sales') }}
    {% if is_incremental() %}
        where order_ts > (select coalesce(max(order_ts), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

products as (
    select * from {{ ref('stg_products') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    s.order_id,
    s.customer_id,
    s.product_id,
    s.order_ts,
    cast(s.order_ts as date)                                                as order_date,
    s.quantity,
    s.unit_price_dollars,
    s.discount_dollars,
    s.gross_amount_dollars,
    round(s.gross_amount_dollars - s.discount_dollars, 2)                   as net_amount_dollars,
    round(
        (s.gross_amount_dollars - s.discount_dollars)
        - (s.quantity * p.cost_dollars),
        2
    )                                                                       as gross_margin_dollars,
    s.channel,
    s.store_id,
    p.category,
    p.subcategory,
    p.cost_dollars,
    p.list_price_dollars,
    c.loyalty_tier,
    c.country,
    ({{ fiscal_quarter('s.order_ts') }})                                    as fiscal_quarter,
    {{ generate_surrogate_key(['s.order_id']) }}                            as sales_sk
from sales s
left join products p on s.product_id = p.product_id
left join customers c on s.customer_id = c.customer_id
