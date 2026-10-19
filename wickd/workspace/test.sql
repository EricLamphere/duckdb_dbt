

select *
from {{ ref('mart_customer_360') }}
limit 10;


select *
from {{ ref('mart_web_analytics') }}