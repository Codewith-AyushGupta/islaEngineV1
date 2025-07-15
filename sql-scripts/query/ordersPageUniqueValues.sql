WITH base_data AS (
    SELECT 
    orderTags,
    orderShippingCity,
    orderShippingCountry,
    orderProvince,
    orderPaymentGatewayNames
    
    FROM read_parquet(
        '${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_orders/*/*.parquet',
        hive_partitioning = true
    )
    WHERE orderCreatedAtIsoYearOnly BETWEEN ${startYear} AND ${endYear}
),
all_tags AS (
    SELECT DISTINCT unnest(orderTags::varchar[]) AS tag
    FROM base_data
),
paymentGateway_tags AS (
    SELECT DISTINCT unnest(orderPaymentGatewayNames::varchar[]) AS payment
    FROM base_data
)
SELECT 
    to_json(array_agg(DISTINCT orderShippingCity)) AS orderShippingCities,
    to_json(array_agg(DISTINCT orderShippingCountry)) AS orderShippingCountries,
    to_json(array_agg(DISTINCT orderProvince)) AS orderProvinces,
    to_json(array_agg(DISTINCT tag)) AS orderTags,
    to_json(array_agg(DISTINCT payment)) AS paymentGateways
FROM base_data, all_tags,paymentGateway_tags ;