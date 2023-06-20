{{
  config(
    materialized = 'incremental',
    unique_key = 'TransactionTypeID',
    )
}}
select *
from {{ ref('dimTransactionTypes_stg') }} tt
{% if is_incremental() %}
  where tt.TransactionTypeID not in (select TransactionTypeID from {{ this }})
{% endif %}