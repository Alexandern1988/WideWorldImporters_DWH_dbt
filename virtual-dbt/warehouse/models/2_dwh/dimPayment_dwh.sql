{{
  config(
    materialized = 'incremental',
    incremental_stratey = 'merge',
    unique_key = 'PaymentKey',
    )
}}
select *
from {{ ref('dimPayment_stg') }} as dp
{% if is_incremental() %}
  where dp.PaymentKey not in (select PaymentKey from {{ this }})
{% endif %}