{{
  config(
    materialized = 'incremental',
    incremental_strategy='merge',
    unique_key = 'InvoiceKey',
    )
}}
select *
from {{ ref('dimInvoice_stg') }} di
{% if is_incremental() %}
  where di.InvoiceKey not in (select InvoiceKey from {{ this }})
{% endif %}