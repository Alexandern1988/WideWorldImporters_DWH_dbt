{{
  config(
    materialized = 'table',
    )
}}
with deliveryMethods as (
    select *
    from {{ source('Application', 'DeliveryMethods') }}
)
select *
from deliveryMethods