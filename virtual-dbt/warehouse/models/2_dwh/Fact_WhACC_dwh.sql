{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'SK_DailyStockItems',
    )
}}
select *
from {{ ref('Fact_WhACC_stg') }} as fa
{% if is_incremental() %}
  where fa.SK_DailyStockItems not in (select SK_DailyStockItems from {{ this }})
{% endif %}