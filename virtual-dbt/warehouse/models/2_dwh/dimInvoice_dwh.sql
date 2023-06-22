{{
  config(
    materialized = 'incremental',
    incremental_strategy='merge',
    unique_key = 'InvoiceID',
    )
}}
select *
from {{ ref('dimInvoice_stg') }} di
{% if is_incremental() %}
  where di.InvoiceID not in (select InvoiceID from {{ this }})
{% endif %}