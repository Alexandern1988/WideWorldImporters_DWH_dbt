{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'SK_ACC',
    )
}}
select * 
from {{ ref('Fact_SalesACC_stg') }} as fs
{% if is_incremental() %}
  where fs.SK_ACC not in (select SK_ACC from {{ this }})
{% endif %}
