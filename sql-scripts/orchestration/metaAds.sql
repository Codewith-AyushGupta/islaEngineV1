/*
*************************************************************************************************************************************
*************************************************************************************************************************************
********************* Step 0: Create Data Lake												***************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/



--THIS IS DONE ON API UPLOAD SNA
-- read from the public s3 file. Then write to the folder structure.
-- SELECT * FROM readJSON(s3), write to paritioned BY year/month/date

/*
*************************************************************************************************************************************
*************************************************************************************************************************************
********************* Step 1: Read from Data Lake												***************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/

-- Step 1: read from data lake

CREATE OR REPLACE TABLE data_lake_metaAds AS
SELECT 
    *
FROM read_ndjson(
    '${s3BucketRoot}/tenants/tenantid=${tenantId}/tablename=metaAds/*/*.jsonl.gz',
    hive_partitioning = true
)
WHERE CAST(date AS DATE) BETWEEN DATE '${startDate}' AND DATE '${endDate}';




/*
*************************************************************************************************************************************
*************************************************************************************************************************************
********************* Step 2: Create Models	(no files here below)									***************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/

-- create orders
CREATE OR REPLACE TABLE model_metaAds AS
SELECT 
    spend::DOUBLE AS spend,
    actions:: JSON[] AS actions,
    cpc::DOUBLE AS cpc,
    action_values :: JSON[] AS action_values,
    purchase_roas :: JSON[] AS purchase_roas,
    ctr::DOUBLE AS ctr,
	cpm::DOUBLE AS cpm,
	clicks::DOUBLE AS clicks,
    outbound_clicks_ctr :: JSON[]  AS outbound_clicks_ctr,
    cost_per_outbound_click :: JSON[]  AS cost_per_outbound_click,
    outbound_clicks :: JSON[]  AS outbound_clicks,
    campaign_name::VARCHAR AS campaign_name,
    date_start::DATE AS date_start,
    date_stop::DATE AS date_stop,
    account_id::VARCHAR AS account_id,
    snapshotDate::DATE AS snapshotDate,
    strftime(snapshotDate, '%Y-%m') AS iso_month,
    strftime(snapshotDate, '%Y') AS iso_year,
    strftime(snapshotDate, '%Y-%m-%d') AS iso_date,
    strftime(snapshotDate, '%G-W%V') AS iso_week
FROM data_lake_metaAds;

/*
*************************************************************************************************************************************
*************************************************************************************************************************************
********************* Step 3: WRITE TO OUTPUT TABLES											***************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/

-- Step 3: output
--


COPY model_metaAds TO '${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=socialMedia/model_metaAds.parquet'
(FORMAT parquet, COMPRESSION zstd);




--Old partition but year is only 41kb so making one file
--COPY model_metaAds TO '${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_metaAds'
--(FORMAT parquet, COMPRESSION zstd, PARTITION_BY (iso_year), OVERWRITE_OR_IGNORE TRUE, FILENAME_PATTERN 'part_{i}');


