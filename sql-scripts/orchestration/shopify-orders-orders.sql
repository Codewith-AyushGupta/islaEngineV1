/*
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
Step 0: Create Data Lake	
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/

--


/*
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
Step 1: Read from Data Lake	 prefix with data_lake_		
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/


-- Step 1: read from data lake
CREATE OR REPLACE TABLE data_lake_shopify_orders_created AS 
SELECT *
FROM read_ndjson('${s3BucketRoot}/tenants/tenantid=${tenantId}/tablename=rawShopifyOrdersCreated/*/*.jsonl.gz',
hive_partitioning = true
)
WHERE CAST(date AS DATE) BETWEEN DATE '${startDate}' AND DATE '${endDate}';

--read updated orders data lake
--CREATE OR REPLACE TABLE data_lake_shopify_orders_created AS 
--SELECT *
--FROM read_ndjson('${s3BucketRoot}/tenants/tenantid=${tenantId}/tablename=rawShopifyOrdersCreated/*/*.jsonl.gz',
--hive_partitioning = true
--)
--WHERE CAST(date AS DATE) BETWEEN DATE '${startDate}' AND DATE '${endDate}';


/*
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
Step 3: Create Models	(no files here below)			
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/


--Format is tableName order_id
-- create orders
CREATE OR REPLACE TABLE model_orders AS 
SELECT
json_extract_string(json, '$.id') AS orderId,
json_extract_string(json, '$.createdAt')::TIMESTAMPTZ AS orderCreatedAt, 
json_extract_string(json, '$.customerJourneySummary.customerOrderIndex')::INT AS orderCustomerOrderIndex, 
json_extract_string(json, '$.customer.id')::VARCHAR AS orderCustomerId, 
json_extract_string(json, '$.customer.displayName')::VARCHAR AS orderCustomerDisplayName, 
json_extract_string(json, '$.customer.firstName')::VARCHAR AS orderCustomerFirstName,
json_extract_string(json, '$.customer.lastName')::VARCHAR AS orderCustomerLastName,
json_extract_string(json, '$.customer.email')::VARCHAR AS orderCustomerEmail, 
json_extract_string(json, '$.totalReceivedSet.shopMoney.amount')::DOUBLE AS orderTotalRecievedShopMoney, 
json_extract_string(json, '$.totalReceivedSet.presentmentMoney.amount')::DOUBLE AS orderTotalRecievedpresentmentMoney, 
json_extract_string(json, '$.totalReceivedSet.presentmentMoney.currencyCode')::VARCHAR AS orderTotalRecievedpresentmentCurrencyCode, 

orderTotalRecievedShopMoney/orderTotalRecievedpresentmentMoney as customerCurrenecyToShopCurrencyConversion,

strftime(orderCreatedAt, '%Y') AS orderCreatedAtIsoYearOnly, 
strftime(orderCreatedAt, '%m') AS orderCreatedAtIsoMonthOnly,  
strftime(orderCreatedAt, '%d') AS orderCreatedAtIsoDayOnly, 
 
 
--json_extract_string(json, '$') AS orderRecordFullJson,

--json_extract_string(json, '$.agreements.edges') ::JSON[] AS orderAgreementsCas,
--json_extract_string(json, '$.transactions') ::JSON[] AS orderTransactions,
--json_extract_string(json, '$.refunds') ::JSON[] AS orderRefunds,

--          fulfillments(first: 2) {
--            legacyResourceId
--            displayStatus
--            totalQuantity
--            name
--            updatedAt
--            createdAt 
--            trackingInfo(first: 3) {
--              company
--              number
--              url
--            }
--          }
--          
          

json_extract_string(json, '$.fulfillments') ::JSON[] AS orderFulfillments, --this breaks api
json_extract_string(orderFulfillments[1].trackingInfo[0], '$.company') ::varchar as firstFulfillmentCompanyNameFoundOnOrder,
json_extract_string(json, '$.transactions') AS orderTransactions,
json_extract_string(json, '$.refunds') AS orderRefunds,
json_extract_string(json, '$.agreements.edges') AS orderAgreements,
json_extract_string(json, '$.paymentGatewayNames') AS orderPaymentGatewayNames,
json_extract_string(json, '$.tags') AS orderTags,
json_extract_string(json, '$.displayFulfillmentStatus') AS orderDisplayFulfillmentStatus,
json_extract_string(json, '$.displayFinancialStatus') AS orderDisplayFinancialStatus,



json_extract_string(json, '$.shippingAddress.countryCode')  AS orderCountryCode,
json_extract_string(json, '$.shippingAddress.provinceCode')  AS orderProvinceCode,

json_extract_string(json, '$.shippingAddress.province')  AS orderProvince, 

json_extract_string(json, '$.shippingAddress.city')  AS orderShippingCity,
json_extract_string(json, '$.shippingAddress.country')  AS orderShippingCountry,

json_extract_string(json, '$.name')  AS orderName


FROM data_lake_shopify_orders_created;


-- create sales items
CREATE OR REPLACE TABLE model_salesItems AS 
(
WITH agreementsUnnested AS(

SELECT *, unnest(orderAgreements :: JSON[]) AS agreement, 
json_extract(agreement, '$.node.sales.edges') :: JSON[] as salesitems 
FROM model_orders
),
salesItemsUnested AS(
SELECT *, unnest(salesitems) as salesitem FROM agreementsUnnested
)

SELECT 

json_extract_string(salesitem, '$.node.lineItem.variant.legacyResourceId')  AS productVariantId,
json_extract_string(salesitem, '$.node.lineItem.product.id')  AS productId,
json_extract_string(salesitem, '$.node.lineItem.name')  AS productName,
json_extract_string(agreement, '$.node.happenedAt')::TIMESTAMPTZ AS happenedAt,


strftime(happenedAt, '%Y') AS agreementHappenedAtIsoYearOnly, 
strftime(happenedAt, '%m') AS agreementHappenedAtIsoMonthOnly,  
strftime(happenedAt, '%d') AS agreementHappenedAtIsoDayOnly, 

CASE
WHEN strftime(happenedAt, '%m') :: INT <4 
THEN 1

WHEN strftime(happenedAt, '%m') :: INT <7
THEN 2

WHEN strftime(happenedAt, '%m') :: INT <10
THEN 3

ELSE 4

END
as agreementHappenedAtIsoQuarterOnly,



--CAST(((CAST(strftime(happenedAt, '%m') AS INT) - 1) / 3 + 1) AS TEXT) AS agreementHappenedAtIsoQuarter,


json_extract_string(salesitem, '$.node.lineType') AS lineType,
json_extract_string(salesitem, '$.node.actionType') AS actionType,
json_extract_string(salesitem, '$.node.id') AS saleId,
json_extract_string(salesitem, '$.node.quantity'):: int AS quantity,
json_extract_string(salesitem, '$.node.lineItem.originalUnitPriceSet.shopMoney.amount') :: double AS originalUnitPriceSet,
json_extract_string(salesitem, '$.node.totalDiscountAmountBeforeTaxes.shopMoney.amount') :: double AS totalDiscountAmountBeforeTaxes,
json_extract_string(salesitem, '$.node.totalDiscountAmountAfterTaxes.shopMoney.amount') :: double AS totalDiscountAmountAfterTaxes,
json_extract_string(salesitem, '$.node.totalTaxAmount.shopMoney.amount') :: double AS totalTaxAmount,
json_extract_string(salesitem, '$.node.totalAmount.shopMoney.amount') :: double AS totalAmount,
json_extract_string(salesitem, '$.node.lineItem.taxLines')  AS taxLines,

list_transform(taxLines::JSON[], taxline -> taxline.rate :: DOUBLE ) as getActualRateValuesInArray,
list_aggregate(getActualRateValuesInArray, 'sum') as sumOfAllTaxLines,

--Order fields
--orderRecordFullJson,
orderId,
orderName,
orderCreatedAt, -- this breaks on api.. original tz varchar would be nice
orderCreatedAtIsoYearOnly,
orderCreatedAtIsoMonthOnly,
orderCreatedAtIsoDayOnly,
orderCountryCode,
orderProvinceCode,
orderCustomerOrderIndex,
orderCustomerId,
orderPaymentGatewayNames,
orderTags,
customerCurrenecyToShopCurrencyConversion



--customer id
 
FROM salesItemsUnested
);



-- transactions


CREATE OR REPLACE TABLE model_transactions AS 
(
WITH transactionsUnested AS (
	SELECT *, 
	unnest(orderTransactions :: JSON[] ) as orderTransaction,  
	json_extract(orderTransaction, '$.fees') AS fees 
	FROM model_orders
),
feesUnnested AS (
	SELECT *, unnest(fees :: JSON[]) as fee FROM transactionsUnested
)

SELECT 

json_extract_string(orderTransaction, '$.createdAt')::TIMESTAMPTZ AS transactionCreatedAt, --wont work well on api
json_extract_string(orderTransaction, '$.createdAt')::VARCHAR AS transactionId,
json_extract_string(orderTransaction, '$.errorCode')::VARCHAR AS transactionErrorCode,
json_extract_string(orderTransaction, '$.kind')::VARCHAR AS transactionKind,
json_extract_string(orderTransaction, '$.formattedGateway')::VARCHAR AS transactionFormattedGateway,

strftime(transactionCreatedAt, '%Y') AS transactionCreatedAtIsoYearOnly, 
strftime(transactionCreatedAt, '%m') AS transactionCreatedAtIsoMonthOnly,  
strftime(transactionCreatedAt, '%d') AS transactionCreatedAtIsoDayOnly, 

fee.amount.amount :: DOUBLE as amount_amount, 
fee.amount.amount :: VARCHAR as amount_CurrencyCode, 

--Order fields
--orderRecordFullJson,
orderId,
orderName,
orderCreatedAt, -- this breaks on api.. original tz varchar would be nice
orderCreatedAtIsoYearOnly,
orderCreatedAtIsoMonthOnly,
orderCreatedAtIsoDayOnly,
orderCountryCode,
orderProvinceCode,
orderCustomerOrderIndex,
orderCustomerId,
orderPaymentGatewayNames,
orderTags,
customerCurrenecyToShopCurrencyConversion

from feesUnnested
);



-- fulfillments
CREATE OR REPLACE TABLE model_fulfillments AS  
SELECT 
json_extract_string(fulfillment, '$.legacyResourceId') as legacyResourceId,
json_extract_string(fulfillment, '$.createdAt') as createdAt,
json_extract_string(fulfillment, '$.totalQuantity') ::INT as totalQuantity, --CAST TO DOUBLE
json_extract_string(fulfillment, '$.createdAt')::TIMESTAMPTZ AS fulfillmentCreatedAt,
strftime(fulfillmentCreatedAt, '%Y') AS fulfillmentCreatedAtIsoYearOnly, 
strftime(fulfillmentCreatedAt, '%m') AS fulfillmentCreatedAtIsoMonthOnly,  
strftime(fulfillmentCreatedAt, '%d') AS fulfillmentCreatedAtIsoDayOnly,
orderId,
orderName,
orderCreatedAt, -- this breaks on api.. original tz varchar would be nice
orderCreatedAtIsoYearOnly,
orderCreatedAtIsoMonthOnly,
orderCreatedAtIsoDayOnly,
orderCountryCode,
orderProvinceCode,
orderCustomerOrderIndex,
orderCustomerId,
orderPaymentGatewayNames,
orderTags,
customerCurrenecyToShopCurrencyConversion

FRom (SELECT * ,unnest(orderFulfillments) as fulfillment FROM model_orders);


/*
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
Step 3: WRITE TO PARQUET OUTPUT TABLES			
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/



-- Sales items 
-- took approx 10 seconds for all
--





COPY model_orders TO '${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_orders'
(FORMAT parquet, 
COMPRESSION zstd, 
PARTITION_BY (orderCreatedAtIsoYearOnly), 
OVERWRITE_OR_IGNORE TRUE, 
FILENAME_PATTERN 
'part_{i}');


-- https://duckdb.org/docs/stable/guides/performance/file_formats.html
COPY model_salesItems TO '${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_salesItems'
(FORMAT parquet, 
COMPRESSION zstd, 
PARTITION_BY (agreementHappenedAtIsoYearOnly), 
OVERWRITE_OR_IGNORE TRUE, 
FILENAME_PATTERN 
'part_{i}');

COPY model_transactions TO '${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_transactions'
(FORMAT parquet, 
COMPRESSION zstd, 
PARTITION_BY (transactionCreatedAtIsoYearOnly), 
OVERWRITE_OR_IGNORE TRUE, 
FILENAME_PATTERN 
'part_{i}');

COPY model_fulfillments TO '${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_fulfillments'
(FORMAT parquet, 
COMPRESSION zstd, 
PARTITION_BY (fulfillmentCreatedAtIsoYearOnly), 
OVERWRITE_OR_IGNORE TRUE, 
FILENAME_PATTERN 
'part_{i}');



SELECT orderProvince, * FROM model_orders;



--fulfillments



--took les than 1 second for small json.


--
--COPY model_salesItems TO '${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_salesItemsBYMONTH'
--(FORMAT parquet, COMPRESSION zstd, PARTITION_BY (agreementHappenedAtIsoYearOnly,agreementHappenedAtIsoMonthOnly), OVERWRITE_OR_IGNORE TRUE, FILENAME_PATTERN 'part_{i}');
--
--


--COPY productCosts TO '${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=poc_productCosts'
--(FORMAT parquet, COMPRESSION zstd, PARTITION_BY (iso_month), OVERWRITE_OR_IGNORE TRUE, FILENAME_PATTERN 'part_{i}');
--
--
--COPY productCosts TO '${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=poc_productCosts'
--(FORMAT parquet, COMPRESSION zstd, PARTITION_BY (iso_month), OVERWRITE_OR_IGNORE TRUE, FILENAME_PATTERN 'part_{i}');
--


