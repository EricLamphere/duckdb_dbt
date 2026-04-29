{% macro fiscal_quarter(date_column, fiscal_year_start_month=2) %}
    (
        with adjusted as (
            select
                {{ date_column }} as dt,
                -- shift month so fiscal year start becomes month 1
                ((month({{ date_column }}) - {{ fiscal_year_start_month }} + 12) % 12) + 1 as fiscal_month,
                -- fiscal year: if we haven't reached the start month yet, it belongs to prior calendar year's FY
                case
                    when month({{ date_column }}) >= {{ fiscal_year_start_month }}
                        then year({{ date_column }}) + 1
                    else year({{ date_column }})
                end as fiscal_year
        )
        select
            'FY' || cast(fiscal_year as varchar) || '-Q' || cast(ceil(fiscal_month / 3.0) as integer)
        from adjusted
    )
{% endmacro %}
