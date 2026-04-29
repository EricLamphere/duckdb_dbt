with
    customers as (
        select
            *
        from
            {{ ref('int_customer_orders') }}
    ),
    -- Session-level aggregates per known customer
    ga_per_customer as (
        select
            customer_id,
            count(session_id) as total_sessions,
            max(session_date) as last_session_date,
            -- Most frequent traffic category (mode approximation)
            mode () within group (
                order by
                    traffic_category
            ) as top_traffic_category
        from
            {{ ref('int_ga_sessions_cleaned') }}
        where
            customer_id is not null
        group by
            1
    )
select
    {{ generate_surrogate_key(['c.customer_id']) }} as customer_sk,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.signup_date,
    c.country,
    c.loyalty_tier,
    c.orders_count,
    c.lifetime_net_revenue,
    c.total_units_purchased,
    c.first_order_date,
    c.last_order_date,
    c.avg_order_value,
    coalesce(g.total_sessions, 0) as total_sessions,
    g.last_session_date,
    g.top_traffic_category,
    case
        when c.last_order_date is not null then cast(current_date - c.last_order_date as integer)
        else null
    end as rfm_recency_days,
    case
        when c.loyalty_tier = 'gold'
        and c.lifetime_net_revenue >= 500 then 'vip'
        when c.loyalty_tier = 'silver'
        and c.lifetime_net_revenue >= 200 then 'loyal'
        when c.orders_count = 0 then 'new'
        when c.last_order_date < current_date - interval '90 days' then 'at_risk'
        else 'regular'
    end as segment
from
    customers c
    left join ga_per_customer g on c.customer_id = g.customer_id