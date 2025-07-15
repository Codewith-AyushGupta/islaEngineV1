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
SELECT 
	tenantId,
	Frequency,
	Cost,
	StartDate,
	EndDate,
	Tags

FROM read_parquet('${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=model_costScheduler/data.parquet');



--Create this list for the passed in DATE Range
CREATE OR REPLACE TABLE api_dateGenerator AS 

SELECT 
	DATE(generate_series) AS iso_date,
	strftime(iso_date, '%Y-%m') AS iso_month,
    strftime(iso_date, '%Y') AS iso_year,
	strftime(iso_date, '%G-W%V') AS iso_week
    
FROM generate_series(DATE '2019-01-01', CURRENT_DATE, INTERVAL '1' DAY);

WITH costScheduler_fields_definitions AS (
	SELECT 
		iso_date,
		iso_month,
		iso_year,
		iso_week,
		tenantId,
		Tags,
		EXTRACT(DAY FROM (DATE_TRUNC('month', api_dateGenerator.iso_date) + INTERVAL 1 MONTH - INTERVAL 1 DAY)) AS numberdaysInCurrentMonth,
		CASE
			WHEN api_dateGenerator.iso_date BETWEEN api_costScheduler.StartDate AND api_costScheduler.EndDate THEN true
			ELSE false
		END as isActiveOnThisDate,
		Case
			WHEN isActiveOnThisDate IS FALSE THEN 0
			WHEN api_costScheduler.Frequency == 'Monthly' THEN api_costScheduler.Cost/numberdaysInCurrentMonth
			WHEN api_costScheduler.Frequency == 'Weekly' THEN api_costScheduler.Cost/7
			WHEN api_costScheduler.Frequency = 'Yearly' THEN api_costScheduler.Cost/365
			WHEN api_costScheduler.Frequency = 'Daily' THEN api_costScheduler.Cost
		--	--If start date Same as current snapshot, and dont divide
			WHEN api_costScheduler.Frequency = 'Just Once' AND api_costScheduler.StartDate = api_dateGenerator.iso_date THEN api_costScheduler.Cost
		END as dailyCost
	FROM api_costScheduler api_costScheduler
	CROSS JOIN api_dateGenerator api_dateGenerator
	WHERE isActiveOnThisDate = true AND cost > 0
),
aggregated_costScheduler AS (
  SELECT 
    ${groupby} AS groupBy,
    SUM(dailyCost * CAST(Tags = 'staff-costs' AS INT)) AS storehero_staffexpense,
    SUM(dailyCost * CAST(Tags = 'marketing-expenses' AS INT)) AS storehero_marketingcostwithoutadspend,
    SUM(dailyCost * CAST(Tags = 'operational-expenses' AS INT)) AS storehero_operationalexpenses
  FROM costScheduler_fields_definitions
  WHERE isActiveOnThisDate = true
  GROUP BY groupBy
)
SELECT * FROM aggregated_costScheduler;




