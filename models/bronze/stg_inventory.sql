with source as (
    select * from {{ ref('raw_inventory_snapshots') }}
)

select
    cast(snapshot_date as date)                         as snapshot_date,
    store_id,
    product_id,
    cast(on_hand_units as integer)                      as on_hand_units,
    cast(reorder_point as integer)                      as reorder_point,
    cast(safety_stock as integer)                       as safety_stock,
    on_hand_units < reorder_point                       as is_below_reorder
from source
