{{
  config(
    materialized = 'table',
    )
}}
with transactions_cte as (
	select 
		ti.StockItemID
		,convert(date, ti.TransactionDate) as TransactionDate
		,count(*) as trans_cnt
	from {{ ref('Fact_WhStockItemsTrans_dwh') }} as ti
	group by ti.StockItemID,ti.TransactionDate
)
select 
    {{ dbt_utils.generate_surrogate_key(['fst.StockItemID', 'dd.DateKey']) }} as SK_DailyStockItems
	,fst.StockItemID
	,dd.DateKey
	,sum(fst.MovementQuatity) as TotalMovementQuantityInDay	
	,sum(case when fst.MovementQuatity > 0 then fst.MovementQuatity end) as TotalEntryMovementQuantityInDay	
	,sum(case when fst.MovementQuatity < 0 then fst.MovementQuatity end) as TotalWriteOffMovementQuantityInDay	
	,max(fst.MovementQuatity) as MaximumMovementQuantityInDay	
	,min(fst.MovementQuatity) as MinimumMovementQuantityInDay	
	,max(fst.RemainingQuantityAfterThisTransaction) as MaximumRemainingMovementQuantityInDay	
	,max(fst.RemainingQuantityAfterThisTransaction) as MinimumRemainingMovementQuantityInDay	
	,sum(fst.RemainingQuantityAfterThisTransaction) as RemainingMovementQuantityInThisDay	
	,sum(case when fst.MovementQuatity is null then 1 else 0 end) as TotalDaysOffCountTillToday	
	,avg(fst.MovementQuatity) as AverageMovementQuantityInThisDay
	,count(*) as TransactionsCount	
	,sum(tc.trans_cnt) as AverageMovementQuantityTillThisDay	
from {{ ref('Fact_WhStockItemsTrans_dwh') }} as fst
	left join {{ ref('dimDate_dwh') }}       as dd on convert(date, fst.TransactionDate) = convert(date, dd.DateKey)
	left join transactions_cte               as tc on fst.StockItemID = tc.StockItemID and convert(date, fst.TransactionDate) = tc.TransactionDate
group by 
	fst.StockItemID
	,dd.DateKey
