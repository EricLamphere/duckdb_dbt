with
    source as (
        select
            *
        from
            {{ source('raw', 'raw_customers') }}
    )
select
    customer_id,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    {{ normalize_email('email') }} as email,
    cast(signup_date as date) as signup_date,
    upper(country) as country,
    lower(loyalty_tier) as loyalty_tier
from
    source