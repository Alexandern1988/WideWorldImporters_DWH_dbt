{{
  config(
    materialized = 'table',
    )
}}
with stateProvinces as (
  select 
    sp.StateProvinceID
    ,sp.StateProvinceCode
    ,sp.StateProvinceName
    ,sp.CountryID
    ,sp.SalesTerritory
    ,sp.LatestRecordedPopulation
    ,sp.LastEditedBy
    ,sp.ValidFrom
    ,sp.ValidTo
  from {{ source('Application', 'StateProvinces') }} as sp
)
select *
from stateProvinces