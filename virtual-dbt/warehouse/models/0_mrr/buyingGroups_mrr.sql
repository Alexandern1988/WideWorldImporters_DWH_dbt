{{
  config(
    materialized = 'table',
    )
}}
with buyingGroups as (
    select *
    from {{ source('Sales', 'BuyingGroups') }}
)
select *
from buyingGroups