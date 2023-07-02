{{
  config(
    materialized = 'table',
    )
}}

with totals_cte as (
	select 
		dc.CustomerID as CustomerKey
		,dp.PeopleKey
		,sum(fc.TotalPurchasePrice) as TotalBuyPrice
		,sum(fc.TransactionCountTillNow) as NumberOfPurchases
		,sum(fc.EstimatedTotalRetrivedProfit) as TotalEstimatedProfit
		,sum(fc.TotalPurchasedTax) as TotalTax
	from {{ ref('Fact_SalesDailyPeriodical_dwh') }} as fc
		left join {{ ref('dimCustomer_dwh') }} as dc on dc.CustomerID = fc.CustomerKey
		left join {{ ref('dimPeople_dwh') }}   as dp on dp.PeopleKey = fc.PeopleKey
	group by dc.CustomerID ,dp.PeopleKey
)		
,last_record_cte as(
	select 
		fsp.CustomerKey
		,fsp.PeopleKey
		,max(fsp.DateKey) as max_datekey
		,max(fsp.AverageBuyAmountTillNow) as max_AverageBuyAmountTillNow
	from {{ ref('Fact_SalesDailyPeriodical_dwh') }} as fsp
	group by fsp.CustomerKey, fsp.PeopleKey
)
select 
    {{ dbt_utils.generate_surrogate_key(['tc.CustomerKey', 'tc.PeopleKey']) }} as SK_ACC
	,tc.CustomerKey
	,tc.PeopleKey
	,tc.TotalBuyPrice
	,tc.NumberOfPurchases
	,tc.TotalEstimatedProfit
	,tc.TotalTax
	,lr.max_AverageBuyAmountTillNow as AverageBuyAmount
from totals_cte as tc 
	join last_record_cte as lr on tc.CustomerKey = lr.CustomerKey and tc.PeopleKey = lr.PeopleKey



