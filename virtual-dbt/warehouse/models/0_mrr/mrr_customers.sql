{{
  config(
    materialized = 'table',
    )
}}

/****** Script for SelectTopNRows command from SSMS  ******/
with customers as (
	SELECT 
	   CustomerID
      ,CustomerName
      ,BillToCustomerID
      ,CustomerCategoryID
      ,BuyingGroupID
      ,PrimaryContactPersonID
      ,AlternateContactPersonID
      ,DeliveryMethodID
      ,DeliveryCityID
      ,PostalCityID
      ,CreditLimit
      ,AccountOpenedDate
      ,StandardDiscountPercentage
      ,IsStatementSent
      ,IsOnCreditHold
      ,PaymentDays
      ,PhoneNumber
      ,FaxNumber
      ,DeliveryRun
      ,RunPosition
      ,WebsiteURL
      ,DeliveryAddressLine1
      ,DeliveryAddressLine2
      ,DeliveryPostalCode
      ,PostalAddressLine1
      ,PostalAddressLine2
      ,PostalPostalCode
      ,LastEditedBy
      ,ValidFrom
      ,ValidTo
  FROM {{ source('Sales', 'Customers') }}
)
select *
from customers

