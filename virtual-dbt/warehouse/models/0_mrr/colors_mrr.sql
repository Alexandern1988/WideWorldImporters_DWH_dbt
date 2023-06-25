{{
  config(
    materialized = 'table',
    )
}}
with colors as (
    select 
        c.ColorID
        ,c.ColorName
    from {{ source('Warehouse', 'Colors') }} as c
)
select *
from colors