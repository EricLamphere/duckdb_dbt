with inventory as (
    select * from {{ ref('stg_inventory') }}
),

with_lag as (
    select
        snapshot_date,
        store_id,
        product_id,
        on_hand_units,
        reorder_point,
        safety_stock,
        is_below_reorder,
        lag(on_hand_units) over (
            partition by store_id, product_id
            order by snapshot_date
        ) as prev_on_hand_units
    from inventory
)

select
    {{ generate_surrogate_key(['snapshot_date', 'store_id', 'product_id']) }} as inventory_sk,
    snapshot_date,
    store_id,
    product_id,
    on_hand_units,
    reorder_point,
    safety_stock,
    is_below_reorder,
    prev_on_hand_units,
    coalesce(on_hand_units - prev_on_hand_units, 0)                           as unit_delta,
    case
        when prev_on_hand_units is null                   then 'initial'
        when on_hand_units > prev_on_hand_units           then 'replenishment'
        when on_hand_units < prev_on_hand_units           then 'sale_or_shrink'
        else                                                   'flat'
    end                                                                       as movement_type
from with_lag
