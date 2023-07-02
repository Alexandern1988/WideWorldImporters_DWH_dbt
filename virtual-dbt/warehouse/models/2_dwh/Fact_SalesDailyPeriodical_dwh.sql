{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'SK_DailyPeriodical',
    )
}}
select *
from {{ ref('Fact_SalesDailyPeriodical_stg') }} as fdp
{% if is_incremental() %}
  where fdp.SK_DailyPeriodical not in (select SK_DailyPeriodical from {{ this }})
{% endif %}