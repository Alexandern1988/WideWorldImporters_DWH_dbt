{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'StockItemTransactionID',
    )
}}
select *
from {{ ref('Fact_WhStockItemsTrans_stg') }} as fit
{% if is_incremental() %}
  where fit.StockItemTransactionID not in (select StockItemTransactionID from {{ this }})
{% endif %}