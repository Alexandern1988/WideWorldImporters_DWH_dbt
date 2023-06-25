{{
  config(
    materialized = 'table',
    )
}}
with packageTypes as (
    select
        pt.PackageTypeID
        ,pt.PackageTypeName
    from {{ source('Warehouse', 'PackageTypes') }} as pt
)
select * from packageTypes