{{
  config(
    materialized = 'incremental',
    unique_key = 'PackageTypeID',
    )
}}
select * 
from {{ ref('dimPackageTypes_stg') }} pt
{% if is_incremental() %}
 where pt.PackageTypeID not in  (select PackageTypeID from {{ this }})
{% endif %}