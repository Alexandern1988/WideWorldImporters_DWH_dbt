{{
  config(
    materialized = 'table',
    )
}}
with stockItemHoldings as (
    SELECT 
        sih.StockItemID
        ,sih.LastCostPrice
        ,sih.QuantityOnHand
        ,sih.LastStocktakeQuantity
        ,sih.TargetStockLevel
        ,sih.BinLocation
        ,sih.LastEditedWhen
        ,sih.ReorderLevel
    from {{ source('Warehouse', 'StockItemHoldings') }} as sih
)
select *
from stockItemHoldings