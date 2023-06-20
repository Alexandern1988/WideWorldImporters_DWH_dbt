{{
  config(
    materialized = 'table',
    )
}}
with customerCategories as (
    select *
    from {{ source('Sales', 'CustomerCategories') }}
)
select *
from customerCategories