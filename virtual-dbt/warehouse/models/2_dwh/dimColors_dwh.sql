{{
  config(
    materialized = 'incremental',
    unique_key = 'ColorID'
    )
}}
select *
from {{ ref('dimColors_stg') }} c
{% if is_incremental() %}
  where c.ColorID not in (select ColorID from {{ this }})
{% endif %}