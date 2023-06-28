{{
  config(
    materialized = 'table',
    )
}}
with keys_cte as (
	select 
		sit.StockItemTransactionID
		,sit.StockItemID
		,si.UnitPackageID
		,si.OuterPackageID
		,si.ColorID
		,dc.sk_customer 
		,sit.CustomerID
		,dic.InvoiceKey
		,sit.SupplierID
		,sit.PurchaseOrderID
		,sit.TransactionTypeID
		,sit.TransactionOccurredWhen as TransactionDate
	from {{ ref('stockItemsTransactions_stg') }}  as sit
		left join {{ ref('dimStockItems_dwh') }}  as si on sit.StockItemID = si.StockItemID
		left join {{ ref('dimCustomer_dwh') }}    as dc on sit.CustomerID = dc.CustomerID
		left join {{ ref('dimInvoice_dwh') }}     as dic on sit.InvoiceID = dic.InvoiceKey
)
,movements_cte as (
	select 
		st.StockItemTransactionID
		,st.StockItemID
		,sum(st.Quantity) as MovementQuatity
		,sum(st.Quantity) + LAG(sum(st.Quantity),1) over (partition by st.StockItemID order by st.StockItemID) as RemainingQuantityAfterThisTransaction
	from {{ ref('stockItemsTransactions_stg') }} as st
	group by st.StockItemTransactionID,st.StockItemID
)
select 
	kc.StockItemTransactionID
	,kc.StockItemID
	,kc.UnitPackageID
	,kc.OuterPackageID
	,kc.ColorID
	,kc.sk_customer
	,kc.CustomerID
	,kc.InvoiceKey
	,kc.SupplierID
	,kc.PurchaseOrderID
	,kc.TransactionTypeID
	,kc.TransactionDate
	,mc.MovementQuatity
	,mc.RemainingQuantityAfterThisTransaction
from keys_cte as kc
	left join movements_cte as mc on kc.StockItemTransactionID = mc.StockItemTransactionID