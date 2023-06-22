{{
  config(
    materialized = 'incremental',
    incremental_strategy='merge',
    unique_key = 'PeopleKey',
    )
}}
with new_records as (
    SELECT 
        dp.PeopleKey
        ,dp.FullName
        ,dp.PreferredName
        ,dp.IsEmployee
        ,dp.IsSalesperson
        ,dp.CurrentPhoneNumber
        ,dp.PreviousePhoneNumber
        ,dp.PhoneEffectiveDate
        ,dp.FaxNumber
        ,dp.CurrentEmailAddress
        ,dp.PreviouseEmailAddress
        ,dp.EmailEffectiveDate
    FROM {{ ref('dimPeople_stg') }} dp
    {% if is_incremental() %}
      where dp.PeopleKey not in (select PeopleKey from {{ this }})
    {% endif %}
)
,scd_merge as (
    SELECT 
         trg.PeopleKey
        ,trg.FullName
        ,trg.PreferredName
        ,trg.IsEmployee
        ,trg.IsSalesperson
        ,case 
            when src.CurrentPhoneNumber != trg.CurrentPhoneNumber and src.CurrentPhoneNumber is not null
            then src.CurrentPhoneNumber
            else trg.CurrentPhoneNumber end as CurrentPhoneNumber
        ,case
            when src.CurrentPhoneNumber != trg.CurrentPhoneNumber and src.CurrentPhoneNumber is not null 
            then trg.CurrentPhoneNumber
            else trg.PreviousePhoneNumber end as PreviousePhoneNumber
        ,case
            when src.CurrentPhoneNumber != trg.CurrentPhoneNumber and src.CurrentPhoneNumber is not null 
            then getdate()
            else trg.PhoneEffectiveDate end as PhoneEffectiveDate
        ,trg.FaxNumber
        ,case
            when src.CurrentEmailAddress != trg.CurrentEmailAddress and src.CurrentEmailAddress is not null 
            then src.CurrentEmailAddress
            else trg.CurrentEmailAddress end as CurrentEmailAddress
        ,case
            when src.CurrentEmailAddress != trg.CurrentEmailAddress and src.CurrentEmailAddress is not null
            then trg.CurrentEmailAddress
            else trg.PreviouseEmailAddress end as PreviouseEmailAddress
        ,case 
            when src.CurrentEmailAddress != trg.CurrentEmailAddress and src.CurrentEmailAddress is not null
            then getdate()
            else trg.EmailEffectiveDate end as EmailEffectiveDate
    FROM {{ this }} as trg join {{ ref('dimPeople_stg') }} src on trg.PeopleKey = src.PeopleKey

)
select * from new_records
union
select * from scd_merge
