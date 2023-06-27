{{
  config(
    materialized = 'table',
    )
}}
with transactions as (
select 
	fst.CustomerKey
	,sum(fst.TransactionAmount) as TotalPurchasePrice
	,sum(fst.TransactionCount) as TransactionCountTillNow
	,sum(fst.TransactionProfit) as EstimatedTotalRetrivedProfit
	,sum(fst.TaxAmount) as TotalPurchasedTax
	,sum(fst.InActiveDay) as InActiveDayCountTillNow
	,sum(fst.TransactionAmount)/sum(fst.TransactionCount) as AverageBuyAmountTillNow
	,fst.LastBuyDateKey
from (
	select 
		 t.DateKey
		,t.CustomerKey
		,t.InvoiceKey
		,t.PeopleKey
		,t.TransactionTypeKey
		,t.paymentMethodKey
		,t.TransactionID
		,t.AmountExcludingTax
		,t.TaxAmount
		,t.TransactionAmount
		,count(t.TransactionID) as TransactionCount
		,t.TransactionProfit
		,case when convert(date, t.DateKey) = convert(date, getdate()) then 0 else 1 end as InActiveDay
		,case when convert(date, t.DateKey) = convert(date, getdate()) then convert(date, getdate()) 
			  else DATEADD(DAY, -1, convert(date, getdate())) end as LastBuyDateKey
	from  {{ ref('Fact_SalesTransactions_dwh') }} as t    
	group by  
		t.DateKey
		,t.CustomerKey
		,t.InvoiceKey
		,t.PeopleKey
		,t.TransactionTypeKey
		,t.paymentMethodKey
		,t.TransactionID
		,t.AmountExcludingTax
		,t.TaxAmount
		,t.TransactionAmount
		,t.TransactionProfit
		 ) fst
group by 
	fst.CustomerKey
	,fst.LastBuyDateKey
)
, invoices as (
select 
	fst.CustomerKey
	,fst.datekey
	,sum(il.Quantity) as TotalNumberOfStock
from {{ ref('InvoiceLines_stg') }} as il
	left join dev.Fact_SalesTransactions_dwh as fst on il.InvoiceID = fst.InvoiceKey
group by fst.CustomerKey, fst.datekey
)
,peoples as (
	select distinct
		dis.CustomerID
		,dp.PeopleKey
	from {{ ref('dimInvoice_dwh') }} as dis
		left join {{ ref('dimPeople_dwh') }} as dp on dis.PrimaryContactPersonID = dp.PeopleKey
)
select 
	{{ dbt_utils.generate_surrogate_key(['trs.CustomerKey', 'ivs.DateKey','pp.PeopleKey']) }} as SK_DailyPeriodical
	,trs.CustomerKey
	,ivs.DateKey
	,pp.PeopleKey
	,trs.TotalPurchasePrice
	,ivs.TotalNumberOfStock
	,trs.TransactionCountTillNow
	,trs.EstimatedTotalRetrivedProfit
	,trs.TotalPurchasedTax
	,trs.InActiveDayCountTillNow
	,trs.AverageBuyAmountTillNow
	,trs.LastBuyDateKey
from transactions trs
	left join invoices as ivs on trs.CustomerKey = ivs.CustomerKey
	left join peoples as pp on trs.CustomerKey = pp.CustomerID
