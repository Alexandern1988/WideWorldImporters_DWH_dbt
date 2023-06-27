{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'SK_SalesTransactions',
    )
}}
select *
from {{ ref('Fact_SalesTransactions_stg') }} as fst
{% if is_incremental() %}
  where  fst.SK_SalesTransactions not in (select SK_SalesTransactions from {{ this }})
{% endif %}