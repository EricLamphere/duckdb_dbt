with
    inventory as (
        select
            *
        from
            {{ ref('int_inventory_movements') }}
    ),
    products as (
        select
            *
        from
            {{ ref('int_product_dimensions') }}
    ),
    -- Average daily units sold per product to estimate days of cover
    daily_sales as (
        select
            product_id,
            {{ safe_divide('sum(quantity)', 'count(distinct order_date)') }} as avg_daily_units_sold
        from
            {{ ref('int_sales_enriched') }}
        group by
            1
    ),
    joined as (
        select
            i.snapshot_date,
            i.store_id,
            i.product_id,
            p.product_name,
            p.category,
            p.price_tier,
            i.on_hand_units,
            i.reorder_point,
            i.safety_stock,
            i.is_below_reorder,
            i.unit_delta,
            i.movement_type,
            coalesce(d.avg_daily_units_sold, 0) as avg_daily_units_sold,
            i.on_hand_units <= i.safety_stock as stockout_risk_flag,
            {{ safe_divide('i.on_hand_units', 'd.avg_daily_units_sold') }} as days_of_cover
        from
            inventory i
            left join products p on i.product_id = p.product_id
            left join daily_sales d on i.product_id = d.product_id
    )
select
    snapshot_date,
    store_id,
    product_id,
    product_name,
    category,
    price_tier,
    on_hand_units,
    reorder_point,
    safety_stock,
    is_below_reorder,
    stockout_risk_flag,
    unit_delta,
    movement_type,
    round(avg_daily_units_sold, 2) as avg_daily_units_sold,
    round(days_of_cover, 1) as days_of_cover
from
    joined