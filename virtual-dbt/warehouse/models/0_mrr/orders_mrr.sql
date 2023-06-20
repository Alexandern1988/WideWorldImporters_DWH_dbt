{{
  config(
    materialized = 'table',
    )
}}
with orders as (
    select *
    from {{ source('Sales', 'Orders') }}
)
select *
from orders