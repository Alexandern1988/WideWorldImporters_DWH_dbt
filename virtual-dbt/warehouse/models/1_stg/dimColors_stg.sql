{{
  config(
    materialized = 'table',
    )
}}
with dimColors as (
    select 
        c.ColorID
        ,c.ColorName
    from {{ ref('colors_mrr') }} as c
)
select *
from dimColors
