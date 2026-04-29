with sales as (
    select * from {{ ref('int_sales_enriched') }}
),

products as (
    select * from {{ ref('int_product_dimensions') }}
),

joined as (
    select
        s.order_date,
        s.fiscal_quarter,
        s.channel,
        p.category,
        p.price_tier,
        s.order_id,
        s.quantity,
        s.gross_amount_dollars,
        s.net_amount_dollars,
        s.gross_margin_dollars,
        s.discount_dollars
    from sales s
    left join products p on s.product_id = p.product_id
)

select
    order_date,
    fiscal_quarter,
    channel,
    category,
    price_tier,
    count(distinct order_id)                                                        as orders,
    sum(quantity)                                                                   as units_sold,
    round(sum(gross_amount_dollars), 2)                                             as gross_revenue,
    round(sum(net_amount_dollars), 2)                                               as net_revenue,
    round(sum(gross_margin_dollars), 2)                                             as gross_margin_dollars,
    round({{ safe_divide('sum(gross_margin_dollars)', 'sum(net_amount_dollars)') }}, 4) as gross_margin_pct,
    round({{ safe_divide('sum(discount_dollars)', 'sum(gross_amount_dollars)') }}, 4)   as avg_discount_pct
from joined
group by 1, 2, 3, 4, 5
