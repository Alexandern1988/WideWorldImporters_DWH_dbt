{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'SK_DailyStockItems',
    )
}}
select *
from Fact_DailyStockItemTrans_stg as dst
{% if is_incremental() %}
  where dst.SK_DailyStockItems not in (select SK_DailyStockItems from {{ this }})
{% endif %}