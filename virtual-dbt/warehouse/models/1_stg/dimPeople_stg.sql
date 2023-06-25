{{
  config(
    materialized = 'table',
    )
}}
with dimPeople as (
    select 
        pp.PersonID as PeopleKey
        ,pp.FullName
        ,pp.PreferredName
        ,pp.IsEmployee
        ,pp.IsSalesperson
        ,pp.PhoneNumber as CurrentPhoneNumber
        ,pp.PhoneNumber as PreviousePhoneNumber
        ,getdate() as PhoneEffectiveDate
        ,pp.FaxNumber
        ,pp.EmailAddress as CurrentEmailAddress
        ,pp.EmailAddress as PreviouseEmailAddress
        ,getdate() as EmailEffectiveDate
    from {{ ref('people_mrr') }} as pp
)
select *
from dimPeople