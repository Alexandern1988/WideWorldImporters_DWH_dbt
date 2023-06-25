{{
  config(
    materialized = 'table',
    )
}}
with lineProfit_agg as (
	select 
		sicl.InvoiceID
		,sum(sicl.LineProfit) as total_lineProfit
	from {{ ref('InvoiceLines_stg') }} sicl
	group by sicl.InvoiceID
)
select 
	dd.DateKey
	,dc.CustomerID as CustomerKey
	,dic.InvoiceKey
	,dp.PeopleKey
	,ct.TransactionTypeID as TransactionTypeKey
	,dpy.PaymentKey as paymentMethodKey
	,ct.CustomerTransactionID as TransactionID
	,ct.AmountExcludingTax
	,ct.TaxAmount
	,TransactionAmount
	,icl.total_lineProfit as TransactionProfit
from dev.dimDate_dwh as dd 
	join {{ ref('customerTransactions_stg') }} as ct on convert(date, dd.DateKey, 120) = ct.TransactionDate
	left join {{ ref('dimInvoice_dwh') }} as dic on ct.CustomerID = dic.CustomerID
	left join {{ ref('dimPeople_dwh') }} as dp on dic.PrimaryContactPersonID = dp.PeopleKey
	left join {{ ref('dimCustomer_dwh') }} as dc on ct.CustomerID = dc.CustomerID and dc.CurrentFlag = 1
	left join {{ ref('dimPayment_dwh') }} as dpy on dpy.PaymentKey = ct.PaymentMethodID
	left join lineProfit_agg as icl on dic.InvoiceKey = icl.InvoiceID
