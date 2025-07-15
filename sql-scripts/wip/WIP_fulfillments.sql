/*
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*Step 0: SELECT only fields needed, do mini field definitionshere and filter on dates from files and create api tables
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/


-- DONT SELECT * 



--PRAGMA enable_profiling='json';
--PRAGMA profiling_output = '/Users/keithrogers/Documents/duckdbpragmout/WIP_lookupmetrics_${scenario_debug}.json';

--CREATE OR REPLACE
--TABLE api_dateGenerator AS 
--
--SELECT
--	DATE(generate_series) AS iso_date,
--	strftime(iso_date, '%Y-%m') AS iso_month,
--	strftime(iso_date, '%Y') AS iso_year,
--	strftime(iso_date, '%G-W%V') AS iso_week
--FROM
--	generate_series(DATE '${greaterThanOrEqualTo}'::DATE, '${lessThanOrEqualTo}'::DATE, INTERVAL '1' DAY);
--
--




CREATE OR REPLACE TABLE api_shippingCostsByCountry AS 
SELECT * FROM read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=lookups/model_shippingCostsByCountry.parquet');


CREATE OR REPLACE TABLE api_orderIdsWithShippingCost AS 
SELECT * FROM read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=lookups/model_orderIdsWithShippingCost.parquet');


CREATE OR REPLACE TABLE api_defaultValues AS 
SELECT * FROM read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=lookups/model_defaultValues.parquet');


CREATE OR REPLACE TABLE api_productCost AS 
SELECT * FROM read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=lookups/model_productCosts.parquet');


CREATE OR REPLACE TABLE api_salesItems AS 
SELECT * FROM read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_salesItems/*/*.parquet', hive_partitioning = true)
WHERE agreementHappenedAtIsoYearOnly BETWEEN '2024' AND '2024';





/*
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*Step 1: Aggregate with CTE's or cross join, what ever is needed.
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/




WITH salesItemsDefinitions as (

SELECT 
si.saleId,
si.orderName,
si.happenedAt, --current api cannot read this BUG 
si.orderCreatedAt, --current api cannot read this BUG
si.actionType,
si.lineType,
si.totalTaxAmount,
si.originalUnitPriceSet,
si.quantity,
si.productVariantId,
si.orderCountryCode,
--pc.productCost,
--d.defaultShippingMargin as shippingMargin,
--strftime(si.happenedAt, '%Y-%m') AS iso_month,
--strftime(si.happenedAt, '%G-%V') AS iso_week,
--strftime(si.happenedAt, '%Y-%m-%d') AS iso_date, 

--Product cost (storehero_totalproductcost_finaldecision_reason)
/*** product cost logic *** (Sold NON RETURN RPODCUT...PRODUCT, ORDER)
 * 1 . If the varaint exsits in the priduct cost lookup, use that. (You will get the cost of 1 profuct)
 * 2. If 1 NULL, THEN WE a value default gross progit margin calcualtiona pi_poc_sh_defaultValues.defaultGrossProfitMargin.
 *    originalUnitPriceSet_WIHTOUTTAX * (1- (storehero_defaultGrossProfitMargin / 100)) AS storehero_perunit_cost_productCostUsingGrossProfitMargin,  

 * 3.If no defaultGrossProfitMargin, then just use originalUnitPriceSet_WIHTOUTTAX
 */


--final price price*qty


--Product cost

COALESCE(CASE WHEN si.actionType != 'RETURN' AND si.lineType != 'SHIPPING' THEN si.totalDiscountAmountBeforeTaxes END, 0)   as shopify_product_discounts,
COALESCE(CASE WHEN si.actionType != 'RETURN' AND si.lineType = 'SHIPPING' THEN si.totalDiscountAmountBeforeTaxes END, 0) as shopify_shipping_discounts,



COALESCE(
CASE 
  WHEN si.actionType = 'RETURN' OR si.lineType != 'PRODUCT' THEN 0
  WHEN si.totalTaxAmount != 0 THEN si.quantity * si.originalUnitPriceSet / (1 + si.sumOfAllTaxLines)
  ELSE si.quantity * si.originalUnitPriceSet
END , 0 ) AS shopify_grosssales,


COALESCE(
	CASE 
		WHEN ABS((EXTRACT(EPOCH FROM si.happenedAt - si.orderCreatedAt) * 1000)) <31000
		AND si.lineType != 'SHIPPING' AND si.actionType != 'RETURN' AND si.lineType != 'GIFT_CARD' THEN 1
		ELSE 0
	END, 0 )
AS shopify_orders,

  
  COALESCE(
    CASE 
      WHEN si.actionType = 'RETURN' AND si.lineType != 'SHIPPING' THEN
        si.totalAmount - si.totalTaxAmount
      ELSE 
        0
    END , 0) 
   AS shopify_product_returns,

 
   COALESCE(
    CASE 
      WHEN si.lineType != 'SHIPPING' THEN
        si.quantity
      ELSE 
        0
    END, 0 )
   AS shopify_netItemsSold,

  
   --cleaner way to filter instead of case when then wlese
--    count() FILTER (i <= 5) AS lte_five,
--    count() FILTER (i % 2 = 1) AS odds
   
    
  COALESCE(
    CASE 
      WHEN si.lineType = 'SHIPPING' THEN
         si.totalAmount - si.totalTaxAmount
      ELSE 
        0
    END, 0 )
   AS shopify_shippingcharges,

   COALESCE(
    
    CASE 
      WHEN si.lineType = 'SHIPPING' THEN
         si.totalTaxAmount
      ELSE 
        0
    END, 0)
   AS shopify_shippingcharges_TaxesOnly,

   COALESCE(si.totalTaxAmount,0) as shopify_taxes,
   
   
shopify_shipping_discounts + shopify_product_discounts as shopify_total_discounts,

shopify_grosssales - shopify_product_discounts + shopify_product_returns  + shopify_taxes + shopify_shippingcharges as shopify_totalsales,

shopify_grosssales - shopify_product_discounts + shopify_product_returns as shopify_netsales,



shippingCostByCountryLookUp.jsonRecord ::json[] as shippingLookups,
list_filter(shippingLookups, lambda x: json_extract_string(x, '$.countryCode')  == si.orderCountryCode ) as filteredToCountryLevel,

(si.totalAmount - si.totalTaxAmount) - si.totalDiscountAmountBeforeTaxes AS paid_amount_without_tax,

CASE
    WHEN si.totalTaxAmount != 0
    AND si.actionType != 'RETURN'
    AND si.lineType = 'PRODUCT' THEN (
        si.originalUnitPriceSet / (1 + si.sumOfAllTaxLines)
    )
    WHEN si.totalTaxAmount = 0
    AND si.actionType != 'RETURN'
    AND si.lineType = 'PRODUCT' THEN (
        si.originalUnitPriceSet
    )
    ELSE 0
END AS original_unit_price_set_without_tax ,

 --what about adjustment is there a product Id on adjustment?

-- NEED TO ADD IN PRODUCTS LOOKUP


CASE
--    -- return type handle ??
   WHEN si.lineType != 'PRODUCT' THEN 0
   WHEN si.productVariantId IS NULL AND defaultValueLookUp.defaultGrossProfitMargin IS NOT NULL THEN original_unit_price_set_without_tax * (1 - (defaultValueLookUp.defaultGrossProfitMargin / 100))
	 WHEN productCostLookup.variantId IS NULL AND defaultValueLookUp.defaultGrossProfitMargin IS NOT NULL THEN original_unit_price_set_without_tax * (1 - (defaultValueLookUp.defaultGrossProfitMargin / 100))
	WHEN productCostLookup.productCost IS NULL AND defaultValueLookUp.defaultGrossProfitMargin  IS NOT NULL THEN original_unit_price_set_without_tax * (1 - (defaultValueLookUp.defaultGrossProfitMargin / 100)) 
	WHEN productCostLookup.productCost <= 0  AND defaultValueLookUp.defaultGrossProfitMargin  IS NOT NULL THEN original_unit_price_set_without_tax * (1 - (defaultValueLookUp.defaultGrossProfitMargin / 100)) 
	 WHEN productCostLookup.productCost > 0 THEN productCostLookup.productCost
	ELSE original_unit_price_set_without_tax
    END
AS storehero_totalproductcost_lookup_value_for_single_unit,
         
  
     	(si.actionType = 'RETURN')::INT * si.quantity *storehero_totalproductcost_lookup_value_for_single_unit AS storehero_totalproductcost_finaldecision,
     	(si.actionType != 'RETURN')::INT * si.quantity *storehero_totalproductcost_lookup_value_for_single_unit AS storehero_return_totalproductcost_finaldecision,
     	

--
--SELECT * FROM api_defaultValues

defaultValueLookUp.defaultShippingMargin as defaultShippingMargin, -- this should be an array and only take up one field.
defaultValueLookUp.defaultReturnMargin as defaultReturnMargin,
defaultValueLookUp.defaultGrossProfitMargin as defaultGrossProfitMargin,
orderIdWithShipping.shippingCost as shippingCostLinkedDirectlyWithOrder,
orderIdWithShipping.orderId as orderIdWithorderId,


--
--CASE 
--	WHEN si.lineType != 'SHIPPING' THEN 0
--	WHEN list_filter(
--	filteredToCountryLevel, lambda x: json_extract_string(x, '$.countryCode')  == si.orderCountryCode AND json_extract_string(x, '$.region') ==  si.orderProvinceCode)[1] 
--END

CASE 
    WHEN si.lineType != 'SHIPPING' THEN 0
    WHEN si.actionType = 'RETURN' THEN 0
    WHEN shippingCostLinkedDirectlyWithOrder IS NOT NULL THEN shippingCostLinkedDirectlyWithOrder

    WHEN list_filter(
             filteredToCountryLevel, 
             x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode 
                  AND json_extract_string(x, '$.region') = si.orderProvinceCode
         )[1] IS NOT NULL 
    THEN list_filter(
             filteredToCountryLevel, 
             x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode 
                  AND json_extract_string(x, '$.region') = si.orderProvinceCode
         )[1].shippingCost
         
     --This one is for ALL regions
   WHEN list_filter(
             filteredToCountryLevel, 
             x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode 
                  AND json_extract_string(x, '$.region') = 'All Regions'
         )[1] IS NOT NULL 
    THEN list_filter(
             filteredToCountryLevel, 
             x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode 
                  AND json_extract_string(x, '$.region') = 'All Regions'
         )[1].shippingCost
      
     WHEN defaultShippingMargin IS NOT NULL THEN defaultShippingMargin
     ELSE shopify_shippingcharges
         
         
END AS storehero_shippingcost_finaldecision_reason,

 


--THIS IS RETURN SHIPPING  SELECT * FROM api_shippingCostsByCountry
--THIS IS RETURN SHIPPING  SELECT * FROM api_shippingCostsByCountry
--THIS IS RETURN SHIPPING  SELECT * FROM api_shippingCostsByCountry
--THIS IS RETURN SHIPPING  SELECT * FROM api_shippingCostsByCountry

CASE 
    WHEN si.lineType != 'SHIPPING' THEN 0
    WHEN si.actionType != 'RETURN' THEN 0

    WHEN list_filter(
             filteredToCountryLevel, 
             x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode 
                  AND json_extract_string(x, '$.region') = si.orderProvinceCode
         )[1] IS NOT NULL 
    THEN list_filter(
             filteredToCountryLevel, 
             x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode 
                  AND json_extract_string(x, '$.region') = si.orderProvinceCode
         )[1].returnShippingCost
         
     --This one is for ALL regions
   WHEN list_filter(
             filteredToCountryLevel, 
             x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode 
                  AND json_extract_string(x, '$.region') = 'All Regions'
         )[1] IS NOT NULL 
    THEN list_filter(
             filteredToCountryLevel, 
             x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode 
                  AND json_extract_string(x, '$.region') = 'All Regions'
         )[1].returnShippingCost
      
     WHEN defaultShippingMargin IS NOT NULL THEN defaultReturnMargin
     ELSE 0
         
END AS storehero_shippingcost_finaldecision_reason,


--these need to be COALSCE HERE as the sums and minuss 
shopify_grosssales - shopify_product_discounts + shopify_product_returns + shopify_shippingcharges AS storehero_netsales


FROM api_salesItems si

--cross join api_shippingCostsByCountry shippingCostByCountryLookUp
--cross join api_defaultValues defaultValueLookUp 
--
LEFT JOIN (SELECT * FROM api_defaultValues LIMIT 1) AS defaultValueLookUp ON true
LEFT JOIN (SELECT * FROM api_shippingCostsByCountry LIMIT 1) AS shippingCostByCountryLookUp ON true
LEFT JOIN api_orderIdsWithShippingCost orderIdWithShipping on si.orderName LIKE '%' || orderIdWithShipping.orderId || '%'
LEFT JOIN api_productCost productCostLookup on si.productVariantId = productCostLookup.variantId 


-- theory: Once i add a date check to these joins, then it is going to be very very slow.



--si.lineType = 'PRODUCT' AND

-- all these left joins takes 13 seconds in total.
)


--SELECT * FROM api_productCost

SELECT * FROM salesItemsDefinitions



