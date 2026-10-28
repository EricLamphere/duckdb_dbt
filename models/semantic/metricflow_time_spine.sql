{{ config(materialized='table') }}

-- One row per day: MetricFlow joins cumulative / grain-to-date / offset metrics to it
select cast(day as date) as date_day
from range(date '2022-01-01', date '2027-01-01', interval 1 day) as t(day)
