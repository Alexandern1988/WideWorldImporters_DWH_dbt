{{
  config(
    materialized = 'incremental',
    incremental_strategy='merge',
    unique_key = 'SK_Customer',
    )
}}

with new_records as (
    select
        src.SK_Customer,
        src.CustomerID,
        src.CustomerName,
        src.CustomerCategoryID,
        src.CustomerCategoryName,
        src.BuyingGroupID,
        src.BuyingGroupName,
        src.AccountOpenedDate,
        src.WebsiteURL,
        src.DeliveryCityID,
        src.DeliveryCityName,
        src.StateProvinceName,
        src.DeliveryAddressLine1,
        src.DeliveryAddressLine2,
        src.DeliveryPostalCode,
        src.PrimaryContactPersonID,
        src.PrimaryContactName,
        src.PhoneNumber,
        src.StartDate,
        src.EndDate,
        src.CurrentFlag
    from {{ ref('dimCustomer_stg_scd') }} as src
    {% if is_incremental() %}
      where src.SK_Customer not in (select SK_Customer from {{ this }})
    {% endif %}
     
)
,updated_records as (
    select
        trg.SK_Customer,
        trg.CustomerID,
        trg.CustomerName,
        trg.CustomerCategoryID,
        trg.CustomerCategoryName,
        trg.BuyingGroupID,
        trg.BuyingGroupName,
        trg.AccountOpenedDate,
        trg.WebsiteURL,
        trg.DeliveryCityID,
        trg.DeliveryCityName,
        trg.StateProvinceName,
        trg.DeliveryAddressLine1,
        trg.DeliveryAddressLine2,
        trg.DeliveryPostalCode,
        trg.PrimaryContactPersonID,
        trg.PrimaryContactName,
        trg.PhoneNumber,
        trg.StartDate,
        src.dbt_valid_to as EndDate,
        0 as CurrentFlag
    from {{ this }} as trg join {{ ref('dimCustomer_stg_scd') }} as src on trg.CustomerID = src.CustomerID
    where trg.PhoneNumber != src.PhoneNumber AND trg.StartDate != src.StartDate
    union
    select
        src.SK_Customer,
        src.CustomerID,
        src.CustomerName,
        src.CustomerCategoryID,
        src.CustomerCategoryName,
        src.BuyingGroupID,
        src.BuyingGroupName,
        src.AccountOpenedDate,
        src.WebsiteURL,
        src.DeliveryCityID,
        src.DeliveryCityName,
        src.StateProvinceName,
        src.DeliveryAddressLine1,
        src.DeliveryAddressLine2,
        src.DeliveryPostalCode,
        src.PrimaryContactPersonID,
        src.PrimaryContactName,
        src.PhoneNumber,
        src.StartDate,
        src.EndDate,
        src.CurrentFlag
    from {{ this }} as trg join {{ ref('dimCustomer_stg') }} as src on trg.CustomerID = src.CustomerID
    where trg.PhoneNumber != src.PhoneNumber AND trg.StartDate != src.StartDate
)
select * from new_records
union
select * from updated_records
