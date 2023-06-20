{{
  config(
    materialized = 'table',
    )
}}
with dimPackageTypes as (
    select 
        pt.PackageTypeID
        ,pt.PackageTypeName
    from {{ ref('packageTypes_mrr') }} pt
)
select * 
from dimPackageTypes