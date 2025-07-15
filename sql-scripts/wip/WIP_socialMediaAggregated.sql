

CREATE OR REPLACE TABLE api_googleAds AS 
SELECT 
	costMicros,
	clicks,
	conversions,
	iso_month,
	iso_date,
	iso_year,
	iso_week
FROM read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=socialMedia/model_googleAds.parquet');


CREATE OR REPLACE TABLE api_tiktok AS 
SELECT 
	spend,
	clicks,
	conversion,
	iso_month,
	iso_date,
	iso_year,
	iso_week
FROM read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=socialMedia/model_tiktok.parquet');


CREATE OR REPLACE TABLE api_metaAds AS 
SELECT 
    spend,
    iso_month,
	iso_date,
	iso_year,
	iso_week,
    clicks,
    list_filter(actions, x -> json_extract_string(x, '$.action_type') = 'purchase') AS filteredActionsList,
    list_transform(filteredActionsList, x -> x.value::DOUBLE) AS getActualRateValuesInArray,
    list_aggregate(getActualRateValuesInArray, 'sum')::DOUBLE AS metaConversion
FROM read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=socialMedia/model_metaAds.parquet');


WITH google_aggregation AS (
    SELECT 
        SUM(COALESCE(costMicros, 0) / 1000000) AS googleAdsSpend,
        SUM(COALESCE(clicks, 0)) AS googleAdsClicks,
        SUM(COALESCE(conversions, 0)) AS googleAdsConversions,
        ${groupBy} AS grouping
        
    FROM api_googleAds
    GROUP BY  grouping
),

 tikTok_aggregation AS (
    SELECT 
        SUM(COALESCE(spend, 0)) AS tikTokSpend,
        SUM(COALESCE(clicks, 0)) AS tikTokClicks,
        SUM(COALESCE(conversion, 0)) AS tikTokConversions,
        ${groupBy} AS grouping
    FROM api_tiktok
    GROUP BY  grouping
),

metaAds_aggregation AS (
    SELECT 
        SUM(COALESCE(spend, 0)) AS metaAdsSpend,
        SUM(COALESCE(clicks, 0)) AS metaAdsClicks,
        SUM(COALESCE(metaConversion, 0.0))::DOUBLE AS metaConversions,
        ${groupBy} AS grouping
    FROM api_metaAds
    GROUP BY  grouping
),

all_aggregation AS (
	SELECT 	
		g.grouping,
		g.googleAdsSpend,
		g.googleAdsClicks,
		g.googleAdsConversions,
		
		t.tikTokSpend,
		t.tikTokClicks,
		t.tikTokConversions,
		
		m.metaAdsSpend,
		m.metaAdsClicks,
		m.metaConversions,
		
		g.googleAdsSpend + m.metaAdsSpend + t.tikTokSpend AS storehero_totaladspend,
		g.googleAdsClicks + m.metaAdsClicks + t.tikTokClicks AS storehero_total_clicks,
		g.googleAdsConversions + m.metaConversions + t.tikTokConversions  AS storehero_total_conversion_number,
		(storehero_total_conversion_number /storehero_total_clicks) * 100 AS storehero_percent_click_to_purchase

	FROM google_aggregation g
	
	FULL Outer join tikTok_aggregation t on g.grouping = t.grouping
	FULL Outer join metaAds_aggregation m on g.grouping = m.grouping
)

SELECT * FROM all_aggregation;
