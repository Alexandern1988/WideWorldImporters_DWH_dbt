{{
  config(
    materialized = 'table',
    )
}}
with dimStockItems as (
    SELECT 
        si.StockItemID
        ,si.StockItemName
        ,si.Brand
        ,si.[Size]
        ,si.IsChillerStock
        ,si.Barcode
        ,si.ColorID
        ,si.SupplierID
        ,si.TaxRate
        ,si.UnitPrice as OriginalUnitPrice
        ,si.UnitPrice as CurrentUnitPrice
        ,si.RecommendedRetailPrice
        ,si.TypicalWeightPerUnit
        ,si.UnitPackageID
        ,si.OuterPackageID
        ,unit_pack.PackageTypeName as UnitPackageTypeName
        ,outer_pack.PackageTypeName as OuterPackageTypeName
        ,getdate() as UnitPriceEffectiveDate
        ,s.SupplierName
        ,c.ColorName
        ,sih.LastCostPrice
        ,sih.QuantityOnHand
        ,sih.LastStocktakeQuantity
        ,sih.TargetStockLevel
        ,sih.BinLocation as OriginalBinLocation
        ,sih.BinLocation as CurrentBinLocation
        ,sih.LastEditedWhen
        ,sih.ReorderLevel
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