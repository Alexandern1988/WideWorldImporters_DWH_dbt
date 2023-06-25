{{
  config(
    materialized = 'table',
    )
}}
with invoices as (
    select *
    from {{ source('Sales', 'Invoices') }}
)
select *
from invoices