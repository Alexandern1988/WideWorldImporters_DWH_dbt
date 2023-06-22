{{
  config(
    materialized = 'table',
    )
}}
with dimPayment as (
    select 
        pm.PaymentMethodID as PaymentKey
        ,pm.PaymentMethodName
    from {{ ref('paymentMethods_mrr') }} as pm
)
select * from dimPayment