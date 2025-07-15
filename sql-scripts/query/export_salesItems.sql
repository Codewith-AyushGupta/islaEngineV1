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

fulfillments as (
SELECT
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
true
),


fulfillmentsAggregated as (
SELECT
${groupby} AS grouping,

SUM(packagingCostFinalDecision) as packagingCostFinalDecision,
SUM(fulfillmentCostFinalDecision) as fulfillmentCostFinalDecision,
SUM(storehero_fulfillmentandpackagingcostcombined) as storehero_fulfillmentandpackagingcostcombined

FROM
fulfillments
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

si.storehero_productcost AS storehero_productcost,
si.storehero_return_productcost as storehero_return_productcost,
si.storehero_handlingfee_aka_packagingfee AS storehero_handlingfee_aka_packagingfee,
ful.storehero_fulfillmentandpackagingcostcombined AS storehero_fulfillmentandpackagingcostcombined,
si.shopify_shippingcharges AS shopify_shippingcharges,
si.shopify_newcustomershippingcharges AS shopify_newcustomershippingcharges,
si.shopify_taxes AS shopify_taxes,
si.storehero_shippingcostwithouttax AS storehero_shippingcostwithouttax,

-- not defined in schema
si.shopify_totalordercount AS shopify_totalordercount,
si.shopify_newcustomerordercount AS shopify_newcustomerordercount,
si.shopify_repeatcustomerordercount AS shopify_repeatcustomerordercount,
si.shopify_newcustomercount AS shopify_newcustomercount,
si.shopify_grosssales AS shopify_grosssales,
si.shopify_newcustomergrosssales AS shopify_newcustomergrosssales,
si.shopify_repeatcustomergrosssales AS shopify_repeatcustomergrosssales,
si.shopify_discounts AS shopify_discounts,
si.shopify_newcustomerdiscounts AS shopify_newcustomerdiscounts,
si.shopify_repeatcustomerdiscounts AS shopify_repeatcustomerdiscounts,
si.shopify_returns AS shopify_returns,
trans.shopify_successfulsaletransactionfeesinshopcurrency AS shopify_successfulsaletransactionfeesinshopcurrency,
--not defined in schema

si.shopify_totalsales AS shopify_totalsales,
si.shopify_newcustomertotalsales AS shopify_newcustomertotalsales,
si.shopify_repeatcustomertotalsales AS shopify_repeatcustomertotalsales,
si.shopify_netsales AS shopify_netsales,
si.shopify_newcustomernetsales AS shopify_newcustomernetsales,
si.shopify_repeatcustomernetsales AS shopify_repeatcustomernetsales,
si.storehero_netsales AS storehero_netsales,
si.storehero_newcustomernetsales AS storehero_newcustomernetsales,
si.storehero_repeatcustomernetsales AS storehero_repeatcustomernetsales,
m.metaAdsSpend AS meta_adspend,
--this will be null here if there is not data on a particualy grouping. we might needs a coalesce for cross join formulas
g.googleAdsSpend AS google_adspend,
t.tikTokSpend AS tiktok_adspend,
g.googleAdsSpend + m.metaAdsSpend + t.tikTokSpend AS storehero_totaladspend,
g.googleAdsClicks + m.metaAdsClicks + t.tikTokClicks AS storehero_total_clicks,
g.googleAdsConversions + m.metaConversions + t.tikTokConversions AS storehero_total_conversion_number,
cs.storehero_marketingcostwithoutadspend AS storehero_marketingcostwithoutadspend,
t.tikTokConversions AS storehero_tiktok_conversion_number,
m.metaConversions AS storehero_metaads_conversion_number,
g.googleAdsConversions AS storehero_googleads_conversion_number,
t.tikTokClicks AS storehero_tiktok_clicks,
m.metaAdsClicks AS storehero_metaads_clicks,
g.googleAdsClicks AS storehero_googleads_clicks,


--WHEN COGS TOGGLE
CASE
	WHEN defaultValueLookUp.TOGGLE_EXCLUDE_COGS IS NULL THEN storehero_productcost
	WHEN defaultValueLookUp.TOGGLE_EXCLUDE_COGS IS FALSE THEN storehero_productcost
	WHEN defaultValueLookUp.TOGGLE_EXCLUDE_COGS IS TRUE THEN storehero_productcost - storehero_return_productcost
	ELSE storehero_productcost
END as storehero_productcost_factored_In_Toggle,


storehero_productcost_factored_In_Toggle +
storehero_handlingfee_aka_packagingfee +
shopify_successfulsaletransactionfeesinshopcurrency +
storehero_fulfillmentandpackagingcostcombined +
storehero_shippingcostwithouttax AS storehero_cogs,


(storehero_netsales - storehero_cogs) AS storehero_grossprofit,
(storehero_newcustomernetsales - storehero_cogs) AS storehero_newcustomergrossprofit,
(storehero_repeatcustomernetsales - storehero_cogs) AS storehero_repeatcustomergrossprofit,
storehero_marketingcostwithoutadspend + storehero_totaladspend AS storehero_marketingcostwithtotaladspend_combined,
cs.storehero_operationalexpenses AS storehero_operationalexpenses,
cs.storehero_staffexpense AS storehero_staffexpense,
cs.storehero_staffandoperationalexpensecombined AS storehero_staffandoperationalexpensecombined,
storehero_netsales -
storehero_cogs-
storehero_marketingcostwithoutadspend-
storehero_totaladspend-
storehero_operationalexpenses-
storehero_staffexpense
AS storehero_contributionmargin,

storehero_netsales -
storehero_cogs -
storehero_marketingcostwithoutadspend -
storehero_totaladspend -
storehero_operationalexpenses-
storehero_staffexpense
AS storehero_netprofit,

si.shopify_netitemssoldonshopifyreport AS shopify_netitemssoldonshopifyreport,
storehero_netsales -
storehero_cogs -
storehero_operationalexpenses-
storehero_staffexpense
AS storehero_bep_marketingspend,
(storehero_total_conversion_number / storehero_total_clicks) * 100 AS storehero_percent_click_to_purchase,
(storehero_newcustomergrossprofit / shopify_newcustomercount) AS storehero_grossprofitpernewcustomer,
(storehero_total_conversion_number / storehero_totaladspend) AS storehero_blended_roas,
si.shopify_totalcustomercount AS shopify_totalcustomercount,
si.shopify_repeatcustomercount AS shopify_repeatcustomercount,
(storehero_repeatcustomergrossprofit / shopify_repeatcustomercount) AS storehero_grossprofitperrepeatcustomer,
si.shopify_aov AS shopify_aov,
si.shopify_newcustomeraov AS shopify_newcustomeraov,
si.shopify_repeatcustomeraov AS shopify_repeatcustomeraov,
(storehero_netsales / storehero_grossprofit) AS storehero_bep_roas,
(storehero_newcustomernetsales / storehero_marketingcostwithtotaladspend_combined) AS storehero_acquisitionmer_with_marketingcostwithtotaladspend_combined,
(si.storehero_newcustomernetsales / storehero_totaladspend) AS storehero_acquisitionmer_usingtotaladspend_only,
(storehero_marketingcostwithtotaladspend_combined / storehero_netsales) AS storehero_marketingpercent_marketingcost_divide_netsales,
(storehero_operationalexpenses + storehero_staffexpense) AS storehero_percent_staffandoperationalexpensecombined,
shopify_totalsales / storehero_marketingcostwithtotaladspend_combined AS storehero_totalsales_mer,
(storehero_netsales / storehero_marketingcostwithtotaladspend_combined) AS storehero_mer_with_marketingcostwithtotaladspend_combined,
(storehero_netsales / storehero_totaladspend) AS storehero_mer_usingtotaladspend_only,
((storehero_grossprofit / storehero_netsales)* 100) AS storehero_grossprofitmargin,
((storehero_grossprofit / storehero_netsales)* 100) AS storehero_percentcogs_cogs_divide_netsales,
(storehero_grossprofit / storehero_bep_marketingspend) AS storehero_bep_mer,
((storehero_netprofit / storehero_netsales)* 100) AS storehero_percentnetprofit_netprofit_divide_netsales,
((storehero_staffexpense / storehero_netsales )* 100) AS storehero_percentstaffcost,
(storehero_totaladspend / shopify_totalordercount) AS storehero_blendedcpa,
(storehero_marketingcostwithtotaladspend_combined / shopify_newcustomerordercount) AS storehero_cpa_marketingcostwithtotaladspend_combined,
(storehero_totaladspend / shopify_newcustomerordercount) AS storehero_cpa_usingtotaladspend_only,
si.storehero_averagenumberofitemsperorder AS storehero_averagenumberofitemsperorder,
(shopify_newcustomertotalsales / storehero_marketingcostwithtotaladspend_combined) AS storehero_newcustomeraquisitioncost_with_marketingcostwithtotaladspend_combined,
(shopify_newcustomertotalsales / storehero_totaladspend) AS storehero_newcustomeraquisitioncost_usingtotaladspend_only,
(storehero_grossprofitpernewcustomer - storehero_cpa_marketingcostwithtotaladspend_combined) AS storehero_newcustomercontributionmargin_with_marketingcostwithtotaladspend_combined,
(storehero_grossprofitpernewcustomer - storehero_cpa_usingtotaladspend_only) AS storehero_newcustomercontributionmargin_usingtotaladspend_only,
((storehero_contributionmargin / storehero_netsales)* 100) AS storehero_percent_contributionmargin,
((shopify_totalsales / shopify_totalsales)* 100) AS storehero_percent_returns,
cs.storehero_marketingcostwithoutadspend AS storehero_marketingcostwithoutadspend
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
groupBy desc