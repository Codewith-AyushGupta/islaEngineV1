

CREATE OR REPLACE
TABLE api_salesItems AS
SELECT
	orderCustomerId,
	orderCustomerOrderIndex,
	orderId,
	totalDiscountAmountBeforeTaxes,
	${groupBy} AS grouping,
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
         -- need to check
        field_shopify_grosssales - field_shopify_product_discounts + field_shopify_product_returns + field_shopify_taxes + field_shopify_shippingcharges as field_shopify_totalsales,
        field_shopify_grosssales - field_shopify_product_discounts + field_shopify_product_returns as field_shopify_netsales,
        field_shopify_grosssales - field_shopify_product_discounts + field_shopify_product_returns + field_shopify_shippingcharges AS field_storehero_netsales,
        (totalAmount - totalTaxAmount) - totalDiscountAmountBeforeTaxes AS field_paid_amount_without_tax,
        ((field_shopify_grosssales - field_shopify_total_discounts)) AS field_shopify_aov,
    
FROM read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_salesItems/*/*.parquet', hive_partitioning = true)
WHERE agreementHappenedAtIsoYearOnly BETWEEN ${startYear} AND ${endYear};

--SELECT * FROM  read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_salesItems/*/*.parquet', hive_partitioning = true)


WITH salesItemsAggregation as (
    SELECT 
    grouping,
    COUNT(DISTINCT (orderCustomerId)) AS shopify_totalcustomercount,
    COUNT(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN orderCustomerId END) AS shopify_newcustomercount,
    COUNT(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN orderCustomerId END) AS shopify_repeatcustomercount,
--    
   COUNT(DISTINCT (orderId)) AS shopify_totalordercount,
   COUNT(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN orderId END) AS shopify_newcustomerordercount,
   COUNT(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN orderId END) AS shopify_repeatcustomerordercount,
--    
--    
   SUM(totalDiscountAmountBeforeTaxes) AS shopify_discounts,
   SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN totalDiscountAmountBeforeTaxes END) AS shopify_newcustomerdiscounts,
   SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN totalDiscountAmountBeforeTaxes END) AS shopify_repeatcustomerdiscounts,
--    
--    
   SUM(field_shopify_grosssales) AS shopify_grosssales,
   SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN field_shopify_grosssales END) AS shopify_newcustomergrosssales,
   SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN field_shopify_grosssales END) AS shopify_repeatcustomergrosssales,
--    
--  
	(SUM(field_shopify_aov)/shopify_totalcustomercount) AS shopify_aov,
   (SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN field_shopify_aov END)/shopify_totalcustomercount) AS shopify_newcustomeraov,
   (SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN field_shopify_aov END)/shopify_totalcustomercount) AS shopify_repeatcustomeraov,
    
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
    
--    
   SUM(field_shopify_shippingcharges) AS shopify_shippingcharges,
   SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN field_shopify_shippingcharges END) AS shopify_newcustomershippingcharges,
   SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN field_shopify_shippingcharges END) AS shopify_repeatcustomershopifyshippingcharges,
    
    
    SUM(field_shopify_product_returns) AS shopify_returns,
--    Not In Dev v-10
--    SUM(DISTINCT CASE WHEN orderCustomerOrderIndex = 1 THEN field_shopify_product_returns END) AS shopify_newcustomerreturns,
--    SUM(DISTINCT CASE WHEN orderCustomerOrderIndex > 1 THEN field_shopify_product_returns END) AS shopify_repeatcustomerreturns,
    
    (shopify_returns/shopify_totalsales)*100 AS storehero_percent_returns
    
 
    FROM api_salesItems si
    GROUP BY grouping
)

SELECT * FROM salesItemsAggregation;