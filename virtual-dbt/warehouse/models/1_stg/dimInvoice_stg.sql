{{
  config(
    materialized = 'table',
    )
}}
with dimInvoice as (
    select 
        ic.InvoiceID as InvoiceKey
        ,ic.CustomerID
        ,cus.CustomerName
        ,ic.BillToCustomerID
        ,bill_cus.CustomerName as BillCustomerName
        ,ic.OrderID
        ,ic.InvoiceDate
        ,ic.PackedByPersonID
        ,pp.FullName as PackedByPersonName
        ,ic.DeliveryMethodID
        ,dm.DeliveryMethodName
        ,ic.DeliveryInstructions as DeliveryAddress
        ,ic.ConfirmedDeliveryTime as DeliveryDate
        ,ic.ConfirmedReceivedBy as ReceivedBy
        ,os.ExpectedDeliveryDate
        ,cus.PrimaryContactPersonID
        ,contact.FullName as PrimaryContactName
        ,contact.PhoneNumber as PrimaryContactPhone
    from sales.invoices as ic
        left join {{ ref('customers_mrr') }}       as cus on ic.CustomerID = cus.CustomerID
        left join {{ ref('customers_mrr') }}       as bill_cus on ic.BillToCustomerID = bill_cus.BillToCustomerID and ic.CustomerID = bill_cus.CustomerID
        left join {{ ref('people_mrr') }}          as pp on ic.PackedByPersonID = pp.PersonID
        left join {{ ref('deliveryMethods_mrr') }} as dm on ic.DeliveryMethodID = dm.DeliveryMethodID
        left join {{ ref('orders_mrr') }}          as os on ic.OrderID = os.OrderID
        left join {{ ref('people_mrr') }}          as contact on cus.PrimaryContactPersonID = contact.PersonID
)
select *
from dimInvoice
