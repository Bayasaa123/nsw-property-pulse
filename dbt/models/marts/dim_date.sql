with days as (

    select explode(
        sequence(date'1990-01-01', date'2030-12-31', interval 1 day)
    ) as date_day
)

select 
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    date_format(date_day, 'MMMM') as month_name,
    cast(date_trunc('month', date_day) as date) as month_start,
    date_format(date_day, 'EEEE') as day_name,
    dayofweek(date_day) in (1, 7) as is_weekend,
    case 
        when month(date_day) >= 7 then year(date_day) + 1
        else year(date_day)
    end as financial_year

from days
