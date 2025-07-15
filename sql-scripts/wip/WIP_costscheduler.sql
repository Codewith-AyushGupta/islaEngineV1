/*
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*Step 0: SELECT only fields needed, do mini field definitionshere and filter on dates from files and create api tables
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/



CREATE OR REPLACE TABLE api_costScheduler AS 
SELECT * FROM read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_costScheduler/data.parquet', hive_partitioning = true);



--Create this list for the passed in DATE Range
CREATE OR REPLACE TABLE api_dateGenerator AS 

SELECT DATE(generate_series) AS isoDate FROM generate_series(DATE '2019-01-01', CURRENT_DATE, INTERVAL '1' DAY);

SELECT 
*,

EXTRACT(DAY FROM (DATE_TRUNC('month', api_dateGenerator.isoDate) + INTERVAL 1 MONTH - INTERVAL 1 DAY)) AS numberdaysInCurrentMonth,
  
CASE
	WHEN api_dateGenerator.isoDate BETWEEN api_costScheduler.StartDate AND api_costScheduler.EndDate THEN true
	ELSE false
END as isActiveOnThisDate,

Case
	WHEN isActiveOnThisDate IS FALSE THEN 0
	WHEN api_costScheduler.Frequency == 'Monthly' THEN api_costScheduler.Cost/numberdaysInCurrentMonth
	WHEN api_costScheduler.Frequency == 'Weekly' THEN api_costScheduler.Cost/7
	WHEN api_costScheduler.Frequency = 'Yearly' THEN api_costScheduler.Cost/365
	WHEN api_costScheduler.Frequency = 'Daily' THEN api_costScheduler.Cost
--	--If start date Same as current snapshot, and dont divide
	WHEN api_costScheduler.Frequency = 'Just Once' AND api_costScheduler.StartDate = api_dateGenerator.isoDate THEN api_costScheduler.Cost
END as dailyCost


FROM api_costScheduler api_costScheduler
CROSS JOIN api_dateGenerator api_dateGenerator
WHERE isActiveOnThisDate = true AND cost > 0


/*
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*Step 1: Aggregate with CTE's or cross join, what ever is needed.
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/


--aggregated_costScheduler AS (
--  SELECT 
--    tenantId,
--    snapshotDate,
--    SUM(dailyCost * CAST(Tags = 'staff-costs' AS INT)) AS staffCost,
--    SUM(dailyCost * CAST(Tags = 'marketing-expenses' AS INT)) AS marketingCost,
--    SUM(dailyCost * CAST(Tags = 'operational-expenses' AS INT)) AS operationalCost
--  FROM deduplicated_costScheduler_snapshots_As_left_join_giving_cartesionproduct
--  GROUP BY snapshotDate, tenantId
--)




