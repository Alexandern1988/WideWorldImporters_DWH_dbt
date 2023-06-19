{{
    config(materialized='table')
}}
with stockGroups as (
    select 
        sg.StockGroupID
        ,sg.StockGroupName
    from {{ source('Warehouse', 'StockGroups') }} as sg
)
select *
from stockGroups