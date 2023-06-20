{{
  config(
    materialized = 'table',
    )
}}
with dimStockItems as (
    SELECT 
        si.StockItemID
        ,si.StockItemName
        ,si.SupplierID
        ,s.SupplierName        
        ,si.Brand
        ,si.[Size]
        ,si.IsChillerStock
        ,si.Barcode
        ,si.ColorID
        ,c.ColorName
        ,si.UnitPackageID 
        ,unit_pack.PackageTypeName as UnitPackageTypeName
        ,si.OuterPackageID
        ,outer_pack.PackageTypeName as OuterPackageTypeName
        ,si.TaxRate
        ,si.UnitPrice as OriginalUnitPrice
        ,si.UnitPrice as CurrentUnitPrice
        ,getdate() as UnitPriceEffectiveDate
        ,si.RecommendedRetailPrice
        ,si.TypicalWeightPerUnit
        ,sih.LastCostPrice
        ,sih.QuantityOnHand
        ,sih.LastStocktakeQuantity
        ,sih.TargetStockLevel
        ,sih.ReorderLevel
        ,sih.BinLocation as OriginalBinLocation
        ,sih.BinLocation as CurrentBinLocation
        ,getdate() as BinLocationEffectiveDate
    from {{ ref('stockItems_mrr') }} as si
        left join {{ ref('stockItemHoldings_mrr') }} as sih        on si.StockItemID = sih.StockItemID
        left join {{ ref('packageTypes_mrr') }}      as unit_pack  on si.UnitPackageID = unit_pack.PackageTypeID
        left join {{ ref('packageTypes_mrr') }}      as outer_pack on si.OuterPackageID = outer_pack.PackageTypeID
        left join {{ ref('colors_mrr') }}            as c          on si.ColorID = c.ColorID
        left join {{ ref('suppliers_mrr') }}         as s          on s.SupplierID = si.SupplierID
)
select *
from dimStockItems