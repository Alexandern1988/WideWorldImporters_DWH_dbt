{{
  config(
    materialized = 'incremental',
    unique_key = 'StockItemID',
    )
}}
with new_records_cte as (
	SELECT 
		 dsi.StockItemID
		,dsi.StockItemName
		,dsi.SupplierID
		,dsi.SupplierName
		,dsi.Brand
		,dsi.Size
		,dsi.IsChillerStock
		,dsi.Barcode
		,dsi.ColorID
		,dsi.ColorName
		,dsi.UnitPackageID
		,dsi.UnitPackageTypeName
		,dsi.OuterPackageID
		,dsi.OuterPackageTypeName
		,dsi.TaxRate
		,dsi.OriginalUnitPrice
		,dsi.CurrentUnitPrice
		,dsi.UnitPriceEffectiveDate
		,dsi.RecommendedRetailPrice
		,dsi.TypicalWeightPerUnit
		,dsi.LastCostPrice
		,dsi.QuantityOnHand
		,dsi.LastStocktakeQuantity
		,dsi.TargetStockLevel
		,dsi.ReorderLevel
		,dsi.OriginalBinLocation
		,dsi.CurrentBinLocation
		,dsi.BinLocationEffectiveDate
	FROM {{ ref('dimStockItems_stg') }} as dsi
    {% if is_incremental() %}
      where dsi.StockItemID not in (select StockItemID from {{ this }})
    {% endif %}
)
,scd_merge_cte as (
	SELECT 
		 trg.StockItemID
		,trg.StockItemName
		,trg.SupplierID
		,trg.SupplierName
		,trg.Brand
		,trg.Size
		,trg.IsChillerStock
		,trg.Barcode
		,trg.ColorID
		,trg.ColorName
		,trg.UnitPackageID
		,trg.UnitPackageTypeName
		,trg.OuterPackageID
		,trg.OuterPackageTypeName
		,trg.TaxRate
		,case 
			when src.CurrentUnitPrice is not null and src.CurrentUnitPrice != trg.CurrentUnitPrice 
			then trg.CurrentUnitPrice 
			else trg.OriginalUnitPrice end as OriginalUnitPrice
		,case
			when src.CurrentUnitPrice is not null and src.CurrentUnitPrice != trg.CurrentUnitPrice
			then src.CurrentUnitPrice
			else trg.CurrentUnitPrice end as CurrentUnitPrice
		,case 
			when src.CurrentUnitPrice is not null and src.CurrentUnitPrice != trg.CurrentUnitPrice 
			then getdate()
			else trg.UnitPriceEffectiveDate end as UnitPriceEffectiveDate
		,trg.RecommendedRetailPrice
		,trg.TypicalWeightPerUnit
		,trg.LastCostPrice
		,trg.QuantityOnHand
		,trg.LastStocktakeQuantity
		,trg.TargetStockLevel
		,trg.ReorderLevel
		,case 
			when src.CurrentBinLocation is not null and src.CurrentBinLocation != trg.CurrentBinLocation
			then trg.CurrentBinLocation
			else trg.OriginalBinLocation end as OriginalBinLocation 
		,case
			when src.CurrentBinLocation is not null and src.CurrentBinLocation != trg.CurrentBinLocation
			then src.CurrentBinLocation
			else trg.CurrentBinLocation end as CurrentBinLocation
		,case 
			when src.CurrentBinLocation is not null and src.CurrentBinLocation != trg.CurrentBinLocation
			then getdate()
			else trg.BinLocationEffectiveDate end as BinLocationEffectiveDate
	FROM {{ this }} as trg 
		join {{ ref('dimStockItems_stg') }} as src on trg.StockItemID = src.StockItemID		
)
select * from new_records_cte 
union 
select * from scd_merge_cte
