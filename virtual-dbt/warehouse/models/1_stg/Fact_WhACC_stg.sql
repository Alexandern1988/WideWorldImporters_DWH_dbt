{{
  config(
    materialized = 'table',
    )
}}
with last_record_cte as (
	select
		t.StockItemID
		,t.TotalDaysOffCountTillToday
		,max(t.DateKey) as max_date
		,avg(t.AverageMovementQuantityTillThisDay) as AverageMovementQuantity	
	from {{ ref('Fact_DailyStockItemTrans_dwh') }} as t
	group by 
		t.StockItemID
		,t.TotalDaysOffCountTillToday
)
select 
	fc.SK_DailyStockItems
	,fc.StockItemID
	,sum(fc.RemainingMovementQuantityInThisDay) as TotalRemainingMovementQuantity	
	,lr.TotalDaysOffCountTillToday
	,lr.AverageMovementQuantity
from {{ ref('Fact_DailyStockItemTrans_dwh') }} as fc
	left join last_record_cte as lr on fc.StockItemID = lr.StockItemID
group by 
	fc.SK_DailyStockItems
	,fc.StockItemID
	,fc.DateKey
	,lr.TotalDaysOffCountTillToday
	,lr.AverageMovementQuantity