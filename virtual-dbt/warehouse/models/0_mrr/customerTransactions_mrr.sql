{{
  config(
    materialized = 'table',
    )
}}
with customerTransactions as (
    select *
    from {{ source('Sales', 'CustomerTransactions') }}
)
select *
from customerTransactions