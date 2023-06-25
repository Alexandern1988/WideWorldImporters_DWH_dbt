{{
  config(
    materialized = 'incremental',
    incremental_strategy='merge',
    unique_key = 'StockGroupID',
    )
}}
select * from {{ ref('dimStockGroup_stg') }} sg
{% if is_incremental() %}
  where sg.StockGroupID not in (select StockGroupID from {{ this }})
{% endif %}