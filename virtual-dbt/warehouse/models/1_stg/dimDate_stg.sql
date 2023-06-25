{{
  config(
    materialized = 'table',
    )
}}
with dimDate as (
    select 
        d.DATE
        ,CONCAT(
            datepart(YEAR, d.date),
            case when LEN(CONVERT(varchar, datepart(MONTH, d.date))) < 2 then concat('0', datepart(MONTH, d.date)) else CONVERT(varchar, datepart(MONTH, d.date)) end,
            case when LEN(CONVERT(varchar, datepart(DAY, d.date))) < 2 then concat('0', datepart(DAY, d.date))   else CONVERT(varchar, datepart(DAY, d.date)) end
        ) as DateKey
        ,YEAR(d.date) as date_year
        ,DATEPART(QUARTER, d.date) as date_quarter
        ,MONTH(d.date) as date_month
        ,DATENAME(MONTH, d.date) as month_name
        ,DAY(d.date) as day_date
        ,DATENAME(WEEKDAY, d.date) as day_name
        ,DATEPART(WEEKDAY, d.date) as date_weekday
        ,DATEPART(WEEK, d.date) as date_week
    from {{ ref('Dates_mrr') }} as d
)
select * from dimDate