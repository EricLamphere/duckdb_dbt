with sessions as (
    select * from {{ ref('stg_ga_sessions') }}
)

select
    session_id,
    customer_id,
    session_start_ts,
    cast(session_start_ts as date)  as session_date,
    source,
    medium,
    campaign,
    device_category,
    page_views,
    session_duration_sec,
    converted,
    customer_id is not null         as is_known_customer,
    page_views <= 1                 as is_bounced,
    case
        when session_duration_sec < 60    then 'under_1m'
        when session_duration_sec < 300   then '1_to_5m'
        else                                   'over_5m'
    end                             as session_duration_bucket,
    case
        when medium = 'cpc'
            or (medium = 'social' and campaign != '(not set)')  then 'paid'
        when medium = 'organic'                                  then 'organic'
        when medium = 'email'                                    then 'email'
        when source = 'direct' or medium = 'none'               then 'direct'
        when medium in ('social', 'referral')                   then 'referral'
        else                                                          'other'
    end                             as traffic_category
from sessions
