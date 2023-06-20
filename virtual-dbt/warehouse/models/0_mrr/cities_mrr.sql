{{
  config(
    materialized = 'table',
    )
}}
with cities as (
  select 
    c.CityID
    ,c.CityName
    ,c.StateProvinceID
    ,c.LatestRecordedPopulation
    ,c.LastEditedBy
    ,c.ValidFrom
    ,c.ValidTo
  from {{ source('Application', 'Cities') }} as c
)
select * 
from cities