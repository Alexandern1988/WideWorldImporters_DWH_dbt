{{
  config(
    materialized = 'table',
    )
}}
with dimTransactionTypes as (
    select 
        tt.TransactionTypeID
        ,tt.TransactionTypeName
    from {{ ref('transactionTypes_mrr') }} tt
)
select *
from dimTransactionTypes
