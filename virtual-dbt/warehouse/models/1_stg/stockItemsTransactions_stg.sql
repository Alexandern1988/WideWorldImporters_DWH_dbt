{{
  config(
    materialized = 'table',
    )
}}
with stockItemsTransactions as (
    select *
    from {{ ref('stockItemTransactions_mrr') }}
)
select *
from stockItemsTransactions