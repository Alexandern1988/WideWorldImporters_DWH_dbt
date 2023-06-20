{{
  config(
    materialized = 'table',
    )
}}
with dimStockGroup as (
    select 
        sg.StockGroupID
        ,sg.StockGroupName
    from {{ ref('stockGroups_mrr') }} as sg
)
select * 
from dimStockGroup