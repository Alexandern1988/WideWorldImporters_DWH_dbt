{{
  config(
    materialized = 'table',
    )
}}
with invoiceLines as (
    select *
    from {{ source('Sales', 'InvoiceLines') }}
)
select *
from invoiceLines