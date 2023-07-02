{{
  config(
    materialized = 'table',
    )
}}
with stockItemsTransactions as (
    select *
    from {{ source('Warehouse', 'StockItemTransactions') }}
)
select *
from stockItemsTransactions
