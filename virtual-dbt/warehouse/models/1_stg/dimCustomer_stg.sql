{{
  config(
    materialized = 'table',
    )
}}
with dimCustomer as (
select 
    cus.CustomerID
    ,cus.CustomerName
    ,cus.CustomerCategoryID
    ,cc.CustomerCategoryName
    ,cus.BuyingGroupID
    ,bg.BuyingGroupName
    ,cus.AccountOpenedDate
    ,cus.WebsiteURL
    ,cus.DeliveryCityID
    ,ct.CityName as DeliveryCityName
    ,sp.StateProvinceName
    ,cus.DeliveryAddressLine1
    ,cus.DeliveryAddressLine2
    ,cus.DeliveryPostalCode
    ,cus.PrimaryContactPersonID
    ,pp.FullName as PrimaryContactName
    ,cus.PhoneNumber
    ,cus.ValidFrom as StartDate
    ,cus.validTo as EndDate
    ,1 as CurrentFlag
from {{ ref('customers_mrr') }} as cus
    left join {{ ref('customerCategories_mrr') }} as cc on cus.CustomerCategoryID = cc.CustomerCategoryID
    left join {{ ref('buyingGroups_mrr') }}       as bg on cus.BuyingGroupID = bg.BuyingGroupID
    left join {{ ref('cities_mrr') }}             as ct on cus.DeliveryCityID = ct.CityID
    left join {{ ref('stateProvinces_mrr') }}     as sp on ct.StateProvinceID = sp.StateProvinceID
    left join {{ ref('people_mrr') }}             as pp on cus.PrimaryContactPersonID = pp.PersonID
)
select * 
from dimCustomer