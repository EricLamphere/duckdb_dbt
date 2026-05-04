with
    sessions as (
        select
            *
        from
            {{ ref('int_ga_sessions_cleaned') }}
    )
select
    session_date,
    traffic_category,
    device_category,
    count(session_id) as sessions,
    sum(
        case
            when is_known_customer then 1
            else 0
        end
    ) as known_customer_sessions,
    sum(
        case
            when converted then 1
            else 0
        end
    ) as conversions,
    sum(
        case
            when is_bounced then 1
            else 0
        end
    ) as bounced_sessions,
    round({{ safe_divide('sum(case when converted then 1 else 0 end)', 'count(session_id)') }}, 4) as conversion_rate,
    round({{ safe_divide('sum(case when is_bounced then 1 else 0 end)', 'count(session_id)') }}, 4) as bounce_rate,
    round(avg(session_duration_sec), 1) as avg_session_duration_sec
from
    sessions
group by
    1,
    2,
    3