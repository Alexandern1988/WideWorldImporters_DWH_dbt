{{
  config(
    materialized = 'incremental',
    incremental_strategy='merge',
    unique_key = 'id',
    )
}}
select *
from {{ ref('dimInvoiceLines_stg') }} as il
{% if is_incremental() %}
  where il.InvoiceLineID not in (select InvoiceLineID from {{ this }})
{% endif %}