{{
  config(
    materialized = 'table',
    )
}}
with people as (
    select *
    from {{ source('Application', 'People') }}
)
select * 
from people