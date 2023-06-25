{{
  config(
    materialized = 'table',
    )
}}
with paymentMethods as (
    select *
    from {{ source('Application', 'PaymentMethods') }}
)
select *
from paymentMethods