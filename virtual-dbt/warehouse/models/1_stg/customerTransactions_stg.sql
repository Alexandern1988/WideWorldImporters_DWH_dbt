{{
  config(
    materialized = 'table',
    )
}}
with customerTransactions as (
select * 
from {{ ref('customerTransactions_mrr') }}
)
select * 
from customerTransactions