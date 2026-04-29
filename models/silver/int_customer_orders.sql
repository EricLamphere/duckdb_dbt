with
    customers as (
        select
            *
        from
            {{ ref('stg_customers') }}
    ),
    sales as (
        select
            *
        from
            {{ ref('int_sales_enriched') }}
    ),
    order_aggregates as (
        select
            customer_id,
            count(distinct order_id) as orders_count,
            sum(net_amount_dollars) as lifetime_net_revenue,
            min(order_date) as first_order_date,
            max(order_date) as last_order_date,
            sum(quantity) as total_units_purchased
        from
            sales
        group by
            1
    )
select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.signup_date,
    c.country,
    c.loyalty_tier,
    coalesce(o.orders_count, 0) as orders_count,
    coalesce(o.lifetime_net_revenue, 0) as lifetime_net_revenue,
    coalesce(o.total_units_purchased, 0) as total_units_purchased,
    o.first_order_date,
    o.last_order_date,
    {{ safe_divide('o.lifetime_net_revenue', 'o.orders_count') }} as avg_order_value
from
    customers c
    left join order_aggregates o on c.customer_id = o.customer_id