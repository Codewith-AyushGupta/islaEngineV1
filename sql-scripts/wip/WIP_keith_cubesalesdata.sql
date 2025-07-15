CREATE OR REPLACE
TABLE api_shippingCostsByCountry AS
SELECT
*
FROM
read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=lookups/model_shippingCostsByCountry.parquet');

CREATE OR REPLACE
TABLE api_orderIdsWithShippingCost AS
SELECT
*
FROM
read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=lookups/model_orderIdsWithShippingCost.parquet');

CREATE OR REPLACE
TABLE api_defaultValues AS
SELECT
*
FROM
read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=lookups/model_defaultValues.parquet');

CREATE OR REPLACE
TABLE api_productCost AS
SELECT
*
FROM
read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=lookups/model_productCosts.parquet');

CREATE OR REPLACE
TABLE api_fulfillments AS
SELECT
*
FROM
read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_fulfillments/*/*.parquet', hive_partitioning = true)
WHERE
fulfillmentCreatedAtIsoYearOnly BETWEEN ${startYear} AND ${endYear} ;
--Partition needed?
-- they are fairly small
CREATE OR REPLACE
TABLE api_transactions AS
SELECT
*,
strftime(transactionCreatedAt AT TIME ZONE '${timeZone}', '%Y-%m-%d') AS date,
strftime(transactionCreatedAt AT TIME ZONE '${timeZone}', '%Y-%m') AS month,
strftime(transactionCreatedAt AT TIME ZONE '${timeZone}', '%Y') AS year,
strftime(transactionCreatedAt AT TIME ZONE '${timeZone}', '%G-W%V') AS week

FROM
read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_transactions/*/*.parquet', hive_partitioning = true)
WHERE
transactionCreatedAtIsoYearOnly BETWEEN ${startYear} AND ${endYear};

CREATE OR REPLACE
TABLE api_googleAds AS
SELECT
COALESCE(costMicros , 0 ) AS costMicros,
COALESCE(clicks , 0 ) AS clicks,
COALESCE(conversions , 0 ) AS conversions,
iso_month AS month,
iso_date AS date,
iso_year AS year,
iso_week AS week
FROM
read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=socialMedia/model_googleAds.parquet')
WHERE
iso_date BETWEEN '${greaterThanOrEqualTo}' AND '${lessThanOrEqualTo}';

CREATE OR REPLACE
TABLE api_tiktok AS
SELECT
COALESCE(spend , 0 ) AS spend,
COALESCE(clicks , 0 ) AS clicks,
COALESCE(conversion , 0 ) AS conversion,
iso_month AS month,
iso_date AS date,
iso_year AS year,
iso_week AS week
FROM
read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=socialMedia/model_tikTok.parquet')
WHERE
iso_date BETWEEN '${greaterThanOrEqualTo}' AND '${lessThanOrEqualTo}';

CREATE OR REPLACE
TABLE api_costScheduler AS
SELECT
tenantId,
Frequency,
Cost,
StartDate,
EndDate,
Tags
FROM
read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_costScheduler/data.parquet');
--Create this list for the passed in DATE Range
CREATE OR REPLACE
TABLE api_dateGenerator AS

SELECT
DATE(generate_series) AS iso_date,
strftime(iso_date, '%Y-%m') AS iso_month,
strftime(iso_date, '%Y') AS iso_year,
strftime(iso_date, '%G-W%V') AS iso_week
FROM
generate_series(DATE '${greaterThanOrEqualTo}'::DATE, '${lessThanOrEqualTo}'::DATE, INTERVAL '1' DAY);

CREATE OR REPLACE
TABLE api_salesItems AS
SELECT
*,
--	COALESCE(orderCustomerId , '' ) AS orderCustomerId,
--	orderName,
--	productVariantId,
--	totalTaxAmount
--	COALESCE(orderCustomerOrderIndex , 0 ) AS orderCustomerOrderIndex,
--	COALESCE(orderId , '' ) AS orderId,
--	COALESCE(totalDiscountAmountBeforeTaxes , 0 ) AS totalDiscountAmountBeforeTaxes,
--- these re temporary -------------
strftime(happenedAt AT TIME ZONE '${timeZone}', '%Y-%m-%d') AS date,
strftime(happenedAt AT TIME ZONE '${timeZone}', '%Y-%m') AS month,
strftime(happenedAt AT TIME ZONE '${timeZone}', '%Y') AS year,
strftime(happenedAt AT TIME ZONE '${timeZone}', '%G-W%V') AS week,
--------------------------------------
${groupby} AS grouping,
orderCreatedAtIsoDayOnly,
COALESCE(
CASE
WHEN actionType != 'RETURN'
AND lineType = 'SHIPPING' THEN totalDiscountAmountBeforeTaxes
END,
0
) AS field_shopify_shipping_discounts,

COALESCE(
CASE
WHEN actionType = 'RETURN'
OR lineType != 'PRODUCT' THEN 0
WHEN totalTaxAmount != 0 THEN quantity * originalUnitPriceSet / (1 + sumOfAllTaxLines)
ELSE quantity * originalUnitPriceSet
END,
0
) AS field_shopify_grosssales,

COALESCE(
CASE
WHEN actionType != 'RETURN'
AND lineType != 'SHIPPING' THEN totalDiscountAmountBeforeTaxes
END,
0
) AS field_shopify_product_discounts,

COALESCE(
CASE
WHEN actionType = 'RETURN'
AND lineType != 'SHIPPING' THEN totalAmount - totalTaxAmount
ELSE 0
END,
0
) AS field_shopify_product_returns,
COALESCE(totalTaxAmount, 0) as field_shopify_taxes,
field_shopify_shipping_discounts + field_shopify_product_discounts as field_shopify_total_discounts,

COALESCE(
CASE
WHEN lineType = 'SHIPPING' THEN totalAmount - totalTaxAmount
ELSE 0
END,
0
) AS field_shopify_shippingcharges,
COALESCE(
CASE
WHEN lineType != 'SHIPPING' THEN quantity
ELSE 0
END,
0
) AS field_shopify_netItemsSold,



-- this is in fullfilment

-- this is on transcation CTE.
-- need to check
field_shopify_grosssales - field_shopify_product_discounts + field_shopify_product_returns + field_shopify_taxes + field_shopify_shippingcharges as field_shopify_totalsales,
field_shopify_grosssales - field_shopify_product_discounts + field_shopify_product_returns as field_shopify_netsales,
field_shopify_grosssales - field_shopify_product_discounts + field_shopify_product_returns + field_shopify_shippingcharges AS field_storehero_netsales

FROM
read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_salesItems/*/*.parquet', hive_partitioning = true)
WHERE
agreementHappenedAtIsoYearOnly BETWEEN ${startYear} AND ${endYear}
AND date BETWEEN '${greaterThanOrEqualTo}' AND '${lessThanOrEqualTo}';

CREATE OR REPLACE
TABLE api_metaAds AS
SELECT
COALESCE(spend , 0 ) AS spend,
COALESCE(clicks , 0 ) AS clicks,
iso_month AS month,
iso_date AS date,
iso_year AS year,
iso_week AS week,
COALESCE(list_filter(actions, x -> json_extract_string(x, '$.action_type') = 'purchase'),[]) AS filteredActionsList,
COALESCE(list_transform(filteredActionsList, x -> x.value::DOUBLE),[]) AS getActualRateValuesInArray,
COALESCE(list_aggregate(getActualRateValuesInArray, 'sum'), 0)::DOUBLE AS metaConversion
FROM
read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=socialMedia/model_metaAds.parquet')
WHERE
iso_date BETWEEN '${greaterThanOrEqualTo}' AND '${lessThanOrEqualTo}';


/***
 * 
 * Define TEMP TABLES FULFILLMENTS
 * 
 */

CREATE OR REPLACE TABLE definition_fulfillments AS
SELECT
f.*,
f.legacyResourceId,
f.createdAt,
f.totalQuantity,
--CAST TO DOUBLE
f.fulfillmentCreatedAt,
list_filter(defaultValueLookUp.finalFulfillmentCost, x -> f.totalQuantity BETWEEN x.minUnits AND x.maxUnits ) as fulfillmentCostMatrixFiltered,

fulfillmentCostMatrixFiltered[1].packagingBaseCostPerOrder ::DOUBLE as packagingBaseCostPerOrder,
fulfillmentCostMatrixFiltered[1].packagingCostPerAdditionalProduct ::DOUBLE as packagingCostPerAdditionalProduct,

fulfillmentCostMatrixFiltered[1].fulFillmentBaseCostPerOrder ::DOUBLE as fulFillmentBaseCostPerOrder,
fulfillmentCostMatrixFiltered[1].fulFillmentCostPerAdditionalProduct ::DOUBLE as fulFillmentCostPerAdditionalProduct,


 packagingBaseCostPerOrder + (f.totalQuantity-1) * packagingCostPerAdditionalProduct as packagingCostFinalDecision,
 fulFillmentBaseCostPerOrder + (f.totalQuantity-1) * fulFillmentCostPerAdditionalProduct as fulfillmentCostFinalDecision,
 
packagingBaseCostPerOrder + fulFillmentBaseCostPerOrder as storehero_fulfillmentandpackagingcostcombined,
 

   
strftime(orderCreatedAt AT TIME ZONE '${timeZone}', '%Y-%m-%d') AS date,
-- Not fulfillment is on order created at date, so partitions might get messed up
strftime(orderCreatedAt AT TIME ZONE '${timeZone}', '%Y-%m') AS month,
-- do we need to partition fulfillments as count will be so low
strftime(orderCreatedAt AT TIME ZONE '${timeZone}', '%Y') AS year,
strftime(orderCreatedAt AT TIME ZONE '${timeZone}', '%G-W%V') AS week
FROM
api_fulfillments f
LEFT JOIN (
SELECT
*
FROM
api_defaultValues
LIMIT 1) AS defaultValueLookUp ON
true;



/***
 * 
 * Define TEMP TABLES definition_salesItems
 * 
 */

CREATE OR REPLACE TABLE definition_salesItems AS
SELECT
si.*,

CASE
WHEN si.totalTaxAmount != 0
AND si.actionType != 'RETURN'
AND si.lineType = 'PRODUCT' THEN (
si.originalUnitPriceSet / (1 + si.sumOfAllTaxLines)
)
WHEN si.totalTaxAmount = 0
AND si.actionType != 'RETURN'
AND si.lineType = 'PRODUCT' THEN (si.originalUnitPriceSet)
ELSE 0
END AS field_original_unit_price_set_without_tax,
CASE
--    -- return type handle ??
	
WHEN si.lineType != 'PRODUCT' THEN 0
WHEN si.productVariantId IS NULL
AND defaultValueLookUp.defaultGrossProfitMargin IS NOT NULL THEN field_original_unit_price_set_without_tax * (1 - (defaultValueLookUp.defaultGrossProfitMargin / 100))
WHEN productCostLookup.variantId IS NULL
AND defaultValueLookUp.defaultGrossProfitMargin IS NOT NULL THEN field_original_unit_price_set_without_tax * (1 - (defaultValueLookUp.defaultGrossProfitMargin / 100))
WHEN productCostLookup.productCost IS NULL
AND defaultValueLookUp.defaultGrossProfitMargin IS NOT NULL THEN field_original_unit_price_set_without_tax * (1 - (defaultValueLookUp.defaultGrossProfitMargin / 100))
WHEN productCostLookup.productCost <= 0
AND defaultValueLookUp.defaultGrossProfitMargin IS NOT NULL THEN field_original_unit_price_set_without_tax * (1 - (defaultValueLookUp.defaultGrossProfitMargin / 100))
WHEN productCostLookup.productCost > 0 THEN productCostLookup.productCost
ELSE field_original_unit_price_set_without_tax
END
AS storehero_totalproductcost_lookup_value_for_single_unit,

(si.actionType = 'RETURN')::INT * si.quantity * storehero_totalproductcost_lookup_value_for_single_unit AS field_storehero_totalproductcost_finaldecision,
(si.actionType != 'RETURN')::INT * si.quantity * storehero_totalproductcost_lookup_value_for_single_unit AS field_storehero_return_totalproductcost_finaldecision,


CASE
	WHEN si.lineType != 'PRODUCT' THEN 0
	WHEN si.productVariantId IS NULL THEN 0
	WHEN productCostLookup.packingFee IS NOT NULL THEN productCostLookup.packingFee
	ELSE 0
END
AS handlingfee_aka_packagingfee_for_single_unit,

(si.actionType != 'RETURN')::INT * si.quantity * handlingfee_aka_packagingfee_for_single_unit AS field_storehero_handlingfee_aka_packagingfee,


shippingCostByCountryLookUp.jsonRecord ::json[] as shippingLookups,
list_filter(shippingLookups, lambda x: json_extract_string(x, '$.countryCode') == si.orderCountryCode ) as filteredToCountryLevel,
defaultValueLookUp.defaultShippingMargin as defaultShippingMargin,
-- this should be an array and only take up one field.
defaultValueLookUp.defaultReturnMargin as defaultReturnMargin,
defaultValueLookUp.defaultGrossProfitMargin as defaultGrossProfitMargin,
orderIdWithShipping.shippingCost as shippingCostLinkedDirectlyWithOrder,
orderIdWithShipping.orderId as orderIdWithorderId,
--Create Shipping Matrix
CASE
WHEN si.lineType = 'SHIPPING'
AND si.actionType != 'RETURN' THEN

json_object(

'shippingCostLinkedDirectlyWithOrder', shippingCostLinkedDirectlyWithOrder,
'countryCode_region',
list_filter(filteredToCountryLevel, x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode AND json_extract_string(x, '$.region') = si.orderProvinceCode)[1].shippingCost,
'countryCode_AllRegions',
list_filter(filteredToCountryLevel, x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode AND json_extract_string(x, '$.region') = 'All Regions')[1].shippingCost,

'defaultShippingMargin', defaultShippingMargin,

'shopify_shippingcharges', field_shopify_shippingcharges --this is field level

)
ELSE NULL
END as shippingCostMatrix,

--Create Shipping Matrix
CASE
WHEN si.lineType = 'SHIPPING'
AND si.actionType = 'RETURN' THEN

json_object(

'countryCode_region',
list_filter(filteredToCountryLevel, x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode AND json_extract_string(x, '$.region') = si.orderProvinceCode)[1].returnShippingCost,
'countryCode_AllRegions',
list_filter(filteredToCountryLevel, x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode AND json_extract_string(x, '$.region') = 'All Regions')[1].returnShippingCost,

'defaultReturnMargin', defaultReturnMargin

)
ELSE NULL
END as returnShippingCostMatrix,


CASE
WHEN si.lineType != 'SHIPPING' THEN 0
WHEN si.actionType = 'RETURN' THEN 0
WHEN json_extract_string(shippingCostMatrix, '$.shippingCostLinkedDirectlyWithOrder') IS NOT NULL THEN json_extract_string(shippingCostMatrix, '$.shippingCostLinkedDirectlyWithOrder') ::DOUBLE
WHEN json_extract_string(shippingCostMatrix, '$.countryCode_region') IS NOT NULL THEN json_extract_string(shippingCostMatrix, '$.countryCode_region') ::DOUBLE
WHEN json_extract_string(shippingCostMatrix, '$.countryCode_AllRegions') IS NOT NULL THEN json_extract_string(shippingCostMatrix, '$.countryCode_AllRegions') ::DOUBLE
WHEN json_extract_string(shippingCostMatrix, '$.defaultShippingMargin') IS NOT NULL THEN json_extract_string(shippingCostMatrix, '$.defaultShippingMargin') ::DOUBLE
ELSE json_extract_string(shippingCostMatrix, '$.field_shopify_shippingcharges') ::DOUBLE
END as storehero_shippingcost_finaldecision,

--THIS IS THE RETURN CALCULATION
CASE
WHEN si.lineType != 'SHIPPING' THEN 0
WHEN si.actionType = 'RETURN' THEN 0

WHEN json_extract_string(returnShippingCostMatrix, '$.countryCode_region') IS NOT NULL THEN json_extract_string(returnShippingCostMatrix, '$.countryCode_region') ::DOUBLE
WHEN json_extract_string(returnShippingCostMatrix, '$.countryCode_AllRegions') IS NOT NULL THEN json_extract_string(returnShippingCostMatrix, '$.countryCode_AllRegions') ::DOUBLE
WHEN json_extract_string(returnShippingCostMatrix, '$.defaultReturnMargin') IS NOT NULL THEN json_extract_string(returnShippingCostMatrix, '$.defaultReturnMargin') ::DOUBLE
ELSE 0
END as storehero_returnshippingcost_finaldecision


FROM
api_salesItems si
LEFT JOIN (
SELECT
*
FROM
api_defaultValues
LIMIT 1) AS defaultValueLookUp ON
true
LEFT JOIN (
SELECT
*
FROM
api_shippingCostsByCountry
LIMIT 1) AS shippingCostByCountryLookUp ON
true
LEFT JOIN api_orderIdsWithShippingCost orderIdWithShipping on
si.orderName LIKE '%' || orderIdWithShipping.orderId || '%'
LEFT JOIN api_productCost productCostLookup on
si.productVariantId = productCostLookup.variantId;



WITH google_aggregation AS (
SELECT
SUM(COALESCE(costMicros, 0) / 1000000) AS googleAdsSpend,
SUM(COALESCE(clicks, 0)) AS googleAdsClicks,
SUM(COALESCE(conversions, 0)) AS googleAdsConversions,
${groupby} AS grouping
FROM
api_googleAds
GROUP BY
grouping
),



salesItemsFieldDefinitions as (
SELECT
si.*,

CASE
WHEN si.totalTaxAmount != 0
AND si.actionType != 'RETURN'
AND si.lineType = 'PRODUCT' THEN (
si.originalUnitPriceSet / (1 + si.sumOfAllTaxLines)
)
WHEN si.totalTaxAmount = 0
AND si.actionType != 'RETURN'
AND si.lineType = 'PRODUCT' THEN (si.originalUnitPriceSet)
ELSE 0
END AS field_original_unit_price_set_without_tax,
CASE
--    -- return type handle ??
	
WHEN si.lineType != 'PRODUCT' THEN 0
WHEN si.productVariantId IS NULL
AND defaultValueLookUp.defaultGrossProfitMargin IS NOT NULL THEN field_original_unit_price_set_without_tax * (1 - (defaultValueLookUp.defaultGrossProfitMargin / 100))
WHEN productCostLookup.variantId IS NULL
AND defaultValueLookUp.defaultGrossProfitMargin IS NOT NULL THEN field_original_unit_price_set_without_tax * (1 - (defaultValueLookUp.defaultGrossProfitMargin / 100))
WHEN productCostLookup.productCost IS NULL
AND defaultValueLookUp.defaultGrossProfitMargin IS NOT NULL THEN field_original_unit_price_set_without_tax * (1 - (defaultValueLookUp.defaultGrossProfitMargin / 100))
WHEN productCostLookup.productCost <= 0
AND defaultValueLookUp.defaultGrossProfitMargin IS NOT NULL THEN field_original_unit_price_set_without_tax * (1 - (defaultValueLookUp.defaultGrossProfitMargin / 100))
WHEN productCostLookup.productCost > 0 THEN productCostLookup.productCost
ELSE field_original_unit_price_set_without_tax
END
AS storehero_totalproductcost_lookup_value_for_single_unit,

(si.actionType = 'RETURN')::INT * si.quantity * storehero_totalproductcost_lookup_value_for_single_unit AS field_storehero_totalproductcost_finaldecision,
(si.actionType != 'RETURN')::INT * si.quantity * storehero_totalproductcost_lookup_value_for_single_unit AS field_storehero_return_totalproductcost_finaldecision,


CASE
	WHEN si.lineType != 'PRODUCT' THEN 0
	WHEN si.productVariantId IS NULL THEN 0
	WHEN productCostLookup.packingFee IS NOT NULL THEN productCostLookup.packingFee
	ELSE 0
END
AS handlingfee_aka_packagingfee_for_single_unit,

(si.actionType != 'RETURN')::INT * si.quantity * handlingfee_aka_packagingfee_for_single_unit AS field_storehero_handlingfee_aka_packagingfee,


shippingCostByCountryLookUp.jsonRecord ::json[] as shippingLookups,
list_filter(shippingLookups, lambda x: json_extract_string(x, '$.countryCode') == si.orderCountryCode ) as filteredToCountryLevel,
defaultValueLookUp.defaultShippingMargin as defaultShippingMargin,
-- this should be an array and only take up one field.
defaultValueLookUp.defaultReturnMargin as defaultReturnMargin,
defaultValueLookUp.defaultGrossProfitMargin as defaultGrossProfitMargin,
orderIdWithShipping.shippingCost as shippingCostLinkedDirectlyWithOrder,
orderIdWithShipping.orderId as orderIdWithorderId,
--Create Shipping Matrix
CASE
WHEN si.lineType = 'SHIPPING'
AND si.actionType != 'RETURN' THEN

json_object(

'shippingCostLinkedDirectlyWithOrder', shippingCostLinkedDirectlyWithOrder,
'countryCode_region',
list_filter(filteredToCountryLevel, x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode AND json_extract_string(x, '$.region') = si.orderProvinceCode)[1].shippingCost,
'countryCode_AllRegions',
list_filter(filteredToCountryLevel, x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode AND json_extract_string(x, '$.region') = 'All Regions')[1].shippingCost,

'defaultShippingMargin', defaultShippingMargin,

'shopify_shippingcharges', field_shopify_shippingcharges --this is field level

)
ELSE NULL
END as shippingCostMatrix,

--Create Shipping Matrix
CASE
WHEN si.lineType = 'SHIPPING'
AND si.actionType = 'RETURN' THEN

json_object(

'countryCode_region',
list_filter(filteredToCountryLevel, x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode AND json_extract_string(x, '$.region') = si.orderProvinceCode)[1].returnShippingCost,
'countryCode_AllRegions',
list_filter(filteredToCountryLevel, x -> json_extract_string(x, '$.countryCode') = si.orderCountryCode AND json_extract_string(x, '$.region') = 'All Regions')[1].returnShippingCost,

'defaultReturnMargin', defaultReturnMargin

)
ELSE NULL
END as returnShippingCostMatrix,


CASE
WHEN si.lineType != 'SHIPPING' THEN 0
WHEN si.actionType = 'RETURN' THEN 0
WHEN json_extract_string(shippingCostMatrix, '$.shippingCostLinkedDirectlyWithOrder') IS NOT NULL THEN json_extract_string(shippingCostMatrix, '$.shippingCostLinkedDirectlyWithOrder') ::DOUBLE
WHEN json_extract_string(shippingCostMatrix, '$.countryCode_region') IS NOT NULL THEN json_extract_string(shippingCostMatrix, '$.countryCode_region') ::DOUBLE
WHEN json_extract_string(shippingCostMatrix, '$.countryCode_AllRegions') IS NOT NULL THEN json_extract_string(shippingCostMatrix, '$.countryCode_AllRegions') ::DOUBLE
WHEN json_extract_string(shippingCostMatrix, '$.defaultShippingMargin') IS NOT NULL THEN json_extract_string(shippingCostMatrix, '$.defaultShippingMargin') ::DOUBLE
ELSE json_extract_string(shippingCostMatrix, '$.field_shopify_shippingcharges') ::DOUBLE
END as storehero_shippingcost_finaldecision,

--THIS IS THE RETURN CALCULATION
CASE
WHEN si.lineType != 'SHIPPING' THEN 0
WHEN si.actionType = 'RETURN' THEN 0

WHEN json_extract_string(returnShippingCostMatrix, '$.countryCode_region') IS NOT NULL THEN json_extract_string(returnShippingCostMatrix, '$.countryCode_region') ::DOUBLE
WHEN json_extract_string(returnShippingCostMatrix, '$.countryCode_AllRegions') IS NOT NULL THEN json_extract_string(returnShippingCostMatrix, '$.countryCode_AllRegions') ::DOUBLE
WHEN json_extract_string(returnShippingCostMatrix, '$.defaultReturnMargin') IS NOT NULL THEN json_extract_string(returnShippingCostMatrix, '$.defaultReturnMargin') ::DOUBLE
ELSE 0
END as storehero_returnshippingcost_finaldecision



FROM
api_salesItems si
LEFT JOIN (
SELECT
*
FROM
api_defaultValues
LIMIT 1) AS defaultValueLookUp ON
true
LEFT JOIN (
SELECT
*
FROM
api_shippingCostsByCountry
LIMIT 1) AS shippingCostByCountryLookUp ON
true
LEFT JOIN api_orderIdsWithShippingCost orderIdWithShipping on
si.orderName LIKE '%' || orderIdWithShipping.orderId || '%'
LEFT JOIN api_productCost productCostLookup on
si.productVariantId = productCostLookup.variantId

),


fulfillmentsAggregated as (
SELECT
${groupby} AS grouping,

SUM(packagingCostFinalDecision) as packagingCostFinalDecision,
SUM(fulfillmentCostFinalDecision) as fulfillmentCostFinalDecision,
SUM(storehero_fulfillmentandpackagingcostcombined) as storehero_fulfillmentandpackagingcostcombined

FROM
definition_fulfillments
GROUP BY grouping
),


transactionsAggregated as (
SELECT
${groupby} AS grouping,
SUM(amount_amount*customerCurrenecyToShopCurrencyConversion) as shopify_successfulSaleTransactionFeesInShopCurrency

FROM
api_transactions
WHERE transactionKind = 'SALE' AND transactionErrorCode IS NULL
GROUP BY grouping
),


--- COMMMMETNED THIS OUT
costScheduler_fields_definitions AS (
SELECT
iso_month AS month,
iso_date AS date,
iso_year AS year,
iso_week AS week,
tenantId,
COALESCE(Tags , '') AS Tags,
EXTRACT(DAY FROM (DATE_TRUNC('month', api_dateGenerator.iso_date) + INTERVAL 1 MONTH - INTERVAL 1 DAY)) AS numberdaysInCurrentMonth,
CASE
WHEN api_dateGenerator.iso_date BETWEEN api_costScheduler.StartDate AND api_costScheduler.EndDate THEN true
ELSE false
END as isActiveOnThisDate,
Case
WHEN isActiveOnThisDate IS FALSE THEN 0
WHEN api_costScheduler.Frequency == 'Monthly' THEN api_costScheduler.Cost / numberdaysInCurrentMonth
WHEN api_costScheduler.Frequency == 'Weekly' THEN api_costScheduler.Cost / 7
WHEN api_costScheduler.Frequency = 'Yearly' THEN api_costScheduler.Cost / 365
WHEN api_costScheduler.Frequency = 'Daily' THEN api_costScheduler.Cost
--	--If start date Same as current snapshot, and dont divide
WHEN api_costScheduler.Frequency = 'Just Once'
AND api_costScheduler.StartDate = api_dateGenerator.iso_date THEN api_costScheduler.Cost
END as dailyCost
FROM
api_costScheduler api_costScheduler
CROSS JOIN api_dateGenerator api_dateGenerator
WHERE
isActiveOnThisDate = true
AND cost > 0
AND iso_date BETWEEN '${greaterThanOrEqualTo}' AND '${lessThanOrEqualTo}'

),
aggregated_costScheduler AS (
SELECT
${groupby} AS grouping,
SUM(dailyCost * CAST(Tags = 'staff-costs' AS INT)) AS storehero_staffexpense,
SUM(dailyCost * CAST(Tags = 'marketing-expenses' AS INT)) AS storehero_marketingcostwithoutadspend,
SUM(dailyCost * CAST(Tags = 'operational-expenses' AS INT)) AS storehero_operationalexpenses,
(storehero_staffexpense + storehero_operationalexpenses) AS storehero_staffandoperationalexpensecombined,
FROM
costScheduler_fields_definitions
GROUP BY
grouping
),

tikTok_aggregation AS (
SELECT
SUM(COALESCE(spend, 0)) AS tikTokSpend,
SUM(COALESCE(clicks, 0)) AS tikTokClicks,
SUM(COALESCE(conversion, 0)) AS tikTokConversions,
${groupby} AS grouping
FROM
api_tiktok
GROUP BY
grouping
),

transaction_aggregation AS (
SELECT
SUM(COALESCE(spend, 0)) AS tikTokSpend,
SUM(COALESCE(clicks, 0)) AS tikTokClicks,
SUM(COALESCE(conversion, 0)) AS tikTokConversions,
${groupby} AS grouping
FROM
api_tiktok
GROUP BY
grouping
),

metaAds_aggregation AS (
SELECT
SUM(COALESCE(spend, 0)) AS metaAdsSpend,
SUM(COALESCE(clicks, 0)) AS metaAdsClicks,
SUM(COALESCE(metaConversion, 0.0))::DOUBLE AS metaConversions,
${groupby} AS grouping
FROM
api_metaAds
GROUP BY
grouping
),

salesItemsAggregation as (
SELECT
grouping,
COUNT(DISTINCT (orderCustomerId)) AS shopify_totalcustomercount,
COUNT(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN orderCustomerId END) AS shopify_newcustomercount,
COUNT(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN orderCustomerId END) AS shopify_repeatcustomercount,
COUNT(DISTINCT (orderId)) AS shopify_totalordercount,
COUNT(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN orderId END) AS shopify_newcustomerordercount,
COUNT(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN orderId END) AS shopify_repeatcustomerordercount,
SUM(totalDiscountAmountBeforeTaxes) AS shopify_discounts,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN totalDiscountAmountBeforeTaxes END) AS shopify_newcustomerdiscounts,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN totalDiscountAmountBeforeTaxes END) AS shopify_repeatcustomerdiscounts,
SUM(field_shopify_grosssales) AS shopify_grosssales,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN field_shopify_grosssales END) AS shopify_newcustomergrosssales,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN field_shopify_grosssales END) AS shopify_repeatcustomergrosssales,
SUM(field_shopify_total_discounts) AS shopify_total_discounts,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN field_shopify_total_discounts END) AS shopify_newcustomertotal_discounts,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN field_shopify_total_discounts END) AS shopify_repeatcustomertotal_discounts,
(shopify_grosssales - shopify_total_discounts)/ shopify_totalordercount AS shopify_aov,
(shopify_newcustomergrosssales - shopify_newcustomertotal_discounts)/ shopify_newcustomerordercount AS shopify_newcustomeraov,
(shopify_repeatcustomergrosssales - shopify_repeatcustomertotal_discounts)/ shopify_repeatcustomerordercount AS shopify_repeatcustomeraov,
--   (SUM(field_shopify_grosssales - field_shopify_total_discounts)/shopify_totalordercount) AS shopify_aov,
--    (shopify_newcustomergrosssales - field_shopify_total_discounts)/shopify_newcustomerordercount AS shopify_newcustomeraov,
--   (SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN (shopify_repeatcustomergrosssales - field_shopify_total_discounts) END)/shopify_repeatcustomerordercount) AS shopify_repeatcustomeraov,

SUM(field_shopify_taxes) AS shopify_taxes,
SUM(field_shopify_totalsales) AS shopify_totalsales,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN field_shopify_totalsales END) AS shopify_newcustomertotalsales,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN field_shopify_totalsales END) AS shopify_repeatcustomertotalsales,
SUM(field_shopify_netsales) AS shopify_netsales,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN field_shopify_netsales END) AS shopify_newcustomernetsales,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN field_shopify_netsales END) AS shopify_repeatcustomernetsales,
SUM(field_storehero_netsales) AS storehero_netsales,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN field_storehero_netsales END) AS storehero_newcustomernetsales,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN field_storehero_netsales END) AS storehero_repeatcustomernetsales,
SUM(field_shopify_shippingcharges) AS shopify_shippingcharges,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN field_shopify_shippingcharges END) AS shopify_newcustomershippingcharges,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN field_shopify_shippingcharges END) AS shopify_repeatcustomershopifyshippingcharges,
SUM(field_shopify_product_returns) AS shopify_returns,
--    Not In Dev v-10
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN field_shopify_product_returns END) AS shopify_newcustomerreturns,
SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN field_shopify_product_returns END) AS shopify_repeatcustomerreturns,
(shopify_returns / shopify_totalsales)* 100 AS storehero_percent_returns,
SUM(field_shopify_netItemsSold) AS shopify_netitemssoldonshopifyreport,
SUM(field_storehero_totalproductcost_finaldecision) AS storehero_productcost,
SUM(field_storehero_return_totalproductcost_finaldecision) as storehero_return_productcost,
SUM(field_storehero_handlingfee_aka_packagingfee) AS storehero_handlingfee_aka_packagingfee,
SUM(storehero_shippingcost_finaldecision) AS storehero_shippingcostwithouttax, --notice change of name
shopify_netitemssoldonshopifyreport / shopify_totalordercount AS storehero_averagenumberofitemsperorder
FROM
salesItemsFieldDefinitions
GROUP BY
grouping
),

finalOutput AS (
SELECT
COALESCE(
CAST(g.grouping AS VARCHAR),
CAST(t.grouping AS VARCHAR),
CAST(m.grouping AS VARCHAR),
CAST(cs.grouping AS VARCHAR),
CAST(si.grouping AS VARCHAR),
CAST(trans.grouping AS VARCHAR),
CAST(ful.grouping AS VARCHAR)

) AS groupBy,
-- this is a workAround for some date grouping issue.

COALESCE(si.storehero_productcost,0) AS storehero_productcost,
COALESCE(si.storehero_return_productcost,0) as storehero_return_productcost,
COALESCE(si.storehero_handlingfee_aka_packagingfee,0) AS storehero_handlingfee_aka_packagingfee,
COALESCE(ful.storehero_fulfillmentandpackagingcostcombined,0) AS storehero_fulfillmentandpackagingcostcombined,
COALESCE(si.shopify_shippingcharges,0) AS shopify_shippingcharges,
COALESCE(si.shopify_newcustomershippingcharges,0) AS shopify_newcustomershippingcharges,
COALESCE(si.shopify_taxes,0) AS shopify_taxes,
COALESCE(si.storehero_shippingcostwithouttax,0) AS storehero_shippingcostwithouttax,

-- not defined in schema
COALESCE(si.shopify_totalordercount,0) AS shopify_totalordercount,
COALESCE(si.shopify_newcustomerordercount,0) AS shopify_newcustomerordercount,
COALESCE(si.shopify_repeatcustomerordercount,0) AS shopify_repeatcustomerordercount,
COALESCE(si.shopify_newcustomercount,0) AS shopify_newcustomercount,
COALESCE(si.shopify_grosssales,0) AS shopify_grosssales,
COALESCE(si.shopify_newcustomergrosssales,0) AS shopify_newcustomergrosssales,
COALESCE(si.shopify_repeatcustomergrosssales,0) AS shopify_repeatcustomergrosssales,
COALESCE(si.shopify_discounts,0) AS shopify_discounts,
COALESCE(si.shopify_newcustomerdiscounts,0) AS shopify_newcustomerdiscounts,
COALESCE(si.shopify_repeatcustomerdiscounts,0) AS shopify_repeatcustomerdiscounts,
COALESCE(si.shopify_returns,0) AS shopify_returns,
COALESCE(trans.shopify_successfulsaletransactionfeesinshopcurrency,0) AS shopify_successfulsaletransactionfeesinshopcurrency,
--not defined in schema

COALESCE(si.shopify_totalsales,0) AS shopify_totalsales,
COALESCE(si.shopify_newcustomertotalsales,0) AS shopify_newcustomertotalsales,
COALESCE(si.shopify_repeatcustomertotalsales,0) AS shopify_repeatcustomertotalsales,
COALESCE(si.shopify_netsales,0) AS shopify_netsales,
COALESCE(si.shopify_newcustomernetsales,0) AS shopify_newcustomernetsales,
COALESCE(si.shopify_repeatcustomernetsales,0) AS shopify_repeatcustomernetsales,
COALESCE(si.storehero_netsales,0) AS storehero_netsales,
COALESCE(si.storehero_newcustomernetsales,0) AS storehero_newcustomernetsales,
COALESCE(si.storehero_repeatcustomernetsales,0) AS storehero_repeatcustomernetsales,
COALESCE(m.metaAdsSpend,0) AS meta_adspend,
--this will be null here if there is not data on a particualy grouping. we might needs a coalesce for cross join formulas
COALESCE(g.googleAdsSpend,0) AS google_adspend,
COALESCE(t.tikTokSpend,0) AS tiktok_adspend,
COALESCE(t.tikTokConversions,0) AS storehero_tiktok_conversion_number,
COALESCE(m.metaConversions,0) AS storehero_metaads_conversion_number,
COALESCE(g.googleAdsConversions,0) AS storehero_googleads_conversion_number,
COALESCE(t.tikTokClicks,0) AS storehero_tiktok_clicks,
COALESCE(m.metaAdsClicks,0) AS storehero_metaads_clicks,
COALESCE(g.googleAdsClicks,0) AS storehero_googleads_clicks,

COALESCE((google_adspend + meta_adspend + tiktok_adspend),0) AS storehero_totaladspend,
COALESCE((storehero_googleads_clicks + storehero_metaads_clicks + storehero_tiktok_clicks),0) AS storehero_total_clicks,
COALESCE((storehero_googleads_conversion_number + storehero_metaads_conversion_number + storehero_tiktok_conversion_number),0) AS storehero_total_conversion_number,

COALESCE(cs.storehero_marketingcostwithoutadspend,0) AS storehero_marketingcostwithoutadspend,


--WHEN COGS TOGGLE
COALESCE(CASE
	WHEN defaultValueLookUp.TOGGLE_EXCLUDE_COGS IS NULL THEN storehero_productcost
	WHEN defaultValueLookUp.TOGGLE_EXCLUDE_COGS IS FALSE THEN storehero_productcost
	WHEN defaultValueLookUp.TOGGLE_EXCLUDE_COGS IS TRUE THEN storehero_productcost - storehero_return_productcost
	ELSE storehero_productcost
END,0 )AS storehero_productcost_factored_In_Toggle,

--- this is workarround ideally not to use COALESCE in join formulas
COALESCE(storehero_productcost_factored_In_Toggle +
storehero_handlingfee_aka_packagingfee +
shopify_successfulsaletransactionfeesinshopcurrency +
storehero_fulfillmentandpackagingcostcombined +
storehero_shippingcostwithouttax , 0) AS storehero_cogs,

--- this is workarround ideally not to use COALESCE in join formulas
COALESCE(storehero_netsales - storehero_cogs,0) AS storehero_grossprofit,
COALESCE(storehero_newcustomernetsales - storehero_cogs,0) AS storehero_newcustomergrossprofit,
COALESCE(storehero_repeatcustomernetsales - storehero_cogs,0) AS storehero_repeatcustomergrossprofit,
COALESCE(storehero_marketingcostwithoutadspend + storehero_totaladspend,0) AS storehero_marketingcostwithtotaladspend_combined,
COALESCE(cs.storehero_operationalexpenses,0) AS storehero_operationalexpenses,
COALESCE(cs.storehero_staffexpense,0) AS storehero_staffexpense,
COALESCE(cs.storehero_staffandoperationalexpensecombined,0) AS storehero_staffandoperationalexpensecombined,

COALESCE(storehero_netsales -
storehero_cogs-
storehero_marketingcostwithoutadspend-
storehero_totaladspend-
storehero_operationalexpenses-
storehero_staffexpense,0)
AS storehero_contributionmargin,

COALESCE(storehero_netsales -
storehero_cogs -
storehero_marketingcostwithoutadspend -
storehero_totaladspend -
storehero_operationalexpenses-
storehero_staffexpense,0)
AS storehero_netprofit,

COALESCE(si.shopify_netitemssoldonshopifyreport,0) AS shopify_netitemssoldonshopifyreport,

COALESCE(storehero_netsales -
storehero_cogs -
storehero_operationalexpenses-
storehero_staffexpense,0)
AS storehero_bep_marketingspend,

-- COALESCE((storehero_total_conversion_number / storehero_total_clicks * 100),0) AS storehero_percent_click_to_purchase,
COALESCE(
  storehero_total_conversion_number / storehero_total_clicks * 100,
  0
) AS storehero_percent_click_to_purchase,
COALESCE(storehero_newcustomergrossprofit / shopify_newcustomercount,0) AS storehero_grossprofitpernewcustomer,
COALESCE(storehero_total_conversion_number / storehero_totaladspend,0) AS storehero_blended_roas,
COALESCE(si.shopify_totalcustomercount,0) AS shopify_totalcustomercount,
COALESCE(si.shopify_repeatcustomercount,0) AS shopify_repeatcustomercount,
COALESCE(storehero_repeatcustomergrossprofit / shopify_repeatcustomercount,0) AS storehero_grossprofitperrepeatcustomer,
COALESCE(si.shopify_aov,0) AS shopify_aov,
COALESCE(si.shopify_newcustomeraov,0) AS shopify_newcustomeraov,
COALESCE(si.shopify_repeatcustomeraov,0) AS shopify_repeatcustomeraov,
COALESCE((storehero_netsales / storehero_grossprofit),0) AS storehero_bep_roas,
COALESCE((storehero_newcustomernetsales / storehero_marketingcostwithtotaladspend_combined),0) AS storehero_acquisitionmer_with_marketingcostwithtotaladspend_combined,
COALESCE((si.storehero_newcustomernetsales / storehero_totaladspend),0) AS storehero_acquisitionmer_usingtotaladspend_only,
COALESCE((storehero_marketingcostwithtotaladspend_combined / storehero_netsales),0) AS storehero_marketingpercent_marketingcost_divide_netsales,
COALESCE((storehero_operationalexpenses + storehero_staffexpense),0) AS storehero_percent_staffandoperationalexpensecombined,
COALESCE(shopify_totalsales / storehero_marketingcostwithtotaladspend_combined,0) AS storehero_totalsales_mer,
COALESCE((storehero_netsales / storehero_marketingcostwithtotaladspend_combined),0) AS storehero_mer_with_marketingcostwithtotaladspend_combined,
COALESCE((storehero_netsales / storehero_totaladspend),0) AS storehero_mer_usingtotaladspend_only,
COALESCE(((storehero_grossprofit / storehero_netsales)* 100),0) AS storehero_grossprofitmargin,
COALESCE(((storehero_grossprofit / storehero_netsales)* 100),0) AS storehero_percentcogs_cogs_divide_netsales,
COALESCE((storehero_grossprofit / storehero_bep_marketingspend),0) AS storehero_bep_mer,
COALESCE(((storehero_netprofit / storehero_netsales)* 100),0) AS storehero_percentnetprofit_netprofit_divide_netsales,
COALESCE(((storehero_staffexpense / storehero_netsales )* 100),0) AS storehero_percentstaffcost,
COALESCE((storehero_totaladspend / shopify_totalordercount),0) AS storehero_blendedcpa,
COALESCE((storehero_marketingcostwithtotaladspend_combined / shopify_newcustomerordercount),0) AS storehero_cpa_marketingcostwithtotaladspend_combined,
COALESCE((storehero_totaladspend / shopify_newcustomerordercount),0) AS storehero_cpa_usingtotaladspend_only,
COALESCE(si.storehero_averagenumberofitemsperorder,0) AS storehero_averagenumberofitemsperorder,
COALESCE((shopify_newcustomertotalsales / storehero_marketingcostwithtotaladspend_combined),0) AS storehero_newcustomeraquisitioncost_with_marketingcostwithtotaladspend_combined,
COALESCE((shopify_newcustomertotalsales / storehero_totaladspend),0) AS storehero_newcustomeraquisitioncost_usingtotaladspend_only,
COALESCE((storehero_grossprofitpernewcustomer - storehero_cpa_marketingcostwithtotaladspend_combined),0) AS storehero_newcustomercontributionmargin_with_marketingcostwithtotaladspend_combined,
COALESCE((storehero_grossprofitpernewcustomer - storehero_cpa_usingtotaladspend_only),0) AS storehero_newcustomercontributionmargin_usingtotaladspend_only,
COALESCE(((storehero_contributionmargin / storehero_netsales)* 100),0) AS storehero_percent_contributionmargin,
COALESCE(((shopify_returns / shopify_totalsales)* 100),0) AS storehero_percent_returns,
COALESCE(cs.storehero_marketingcostwithoutadspend,0) AS storehero_marketingcostwithoutadspend
FROM
google_aggregation g
FULL Outer join tikTok_aggregation t on
g.grouping = t.grouping
FULL Outer join metaAds_aggregation m on
g.grouping = m.grouping
FULL Outer join salesItemsAggregation si on
g.grouping = si.grouping
FULL Outer join aggregated_costScheduler cs on
g.grouping = cs.grouping
FULL Outer join transactionsAggregated trans on
g.grouping = trans.grouping

FULL Outer join fulfillmentsAggregated ful on
g.grouping = ful.grouping

LEFT JOIN (
SELECT
*
FROM
api_defaultValues
LIMIT 1) AS defaultValueLookUp ON
true

)

SELECT
*
FROM
finalOutput
Order BY
groupBy desc;