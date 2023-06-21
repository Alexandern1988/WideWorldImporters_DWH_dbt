{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'DateKey'
    )
}}
select *
from {{ ref('dimDate_stg') }}
{% if is_incremental() %}
  where DateKey not in (select DateKey from {{ this }})
{% endif %}