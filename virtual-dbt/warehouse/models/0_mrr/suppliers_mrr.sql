{{
  config(
    materialized = 'table',
    )
}}
with suppliers as (
    SELECT 
        s.SupplierID
        ,SupplierName
    from {{ source('Purchasing', 'Suppliers') }} as s
)
select *
from suppliers