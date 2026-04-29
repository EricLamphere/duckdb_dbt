with
    source as (
        select
            *
        from
            {{ ref('raw_ga_sessions') }}
    )
select
    session_id,
    nullif(customer_id, '') as customer_id,
    cast(session_start_ts as timestamp) as session_start_ts,
    lower(source) as source,
    lower(medium) as medium,
    coalesce(nullif(trim(campaign), ''), '(not set)') as campaign,
    lower(device_category) as device_category,
    cast(page_views as integer) as page_views,
    cast(session_duration_sec as integer) as session_duration_sec,
    cast(converted as boolean) as converted
from
    source