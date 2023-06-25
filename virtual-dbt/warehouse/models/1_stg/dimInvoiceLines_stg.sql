{{
  config(
    materialized = 'table',
    )
}}
with invoiceLines as (
    select *
    from {{ ref('invoiceLines_mrr') }}
)
select * 
from invoiceLines