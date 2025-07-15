/*
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*Step 0: Create Data Lake	
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/


--THIS IS DONE ON API UPLOAD SNA
-- read from the public s3 file. Then write to the folder structure.
-- SELECT * FROM readJSON(s3), write to paritioned BY year/month/date

--what is partition strategy of this lake??


--Match and merge is done all at once.

/*
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*Step 1: Read from data lake	
*************************************************************************************************************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/
CREATE OR REPLACE TABLE data_lake_defaultValues AS
SELECT 
    *
    --mind the wild card here
FROM read_ndjson(
'${s3BucketRoot}/tenants/tenantid=${tenantId}/tablename=defaultsnapshot_defaultValues/date=9999-99-99/*.jsonl.gz',
hive_partitioning = true
);

--Read from existing compacted for this partition?



/*
*************************************************************************************************************************************
*************************************************************************************************************************************
********************* Step 2: Create Models	(no files here below)							***************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/

CREATE OR REPLACE TABLE model_defaultValues AS 

SELECT 
defaultGrossProfitMargin::double as defaultGrossProfitMargin,
defaultShippingMargin::double as defaultShippingMargin,
defaultReturnMargin::double as defaultReturnMargin,
true as TOGGLE_EXCLUDE_COGS,

Case
	WHEN json_exists(defaultFulfillmentCosts, '$.baseCostPerOrder') THEN 'simplePricing'
	ELSE 'variablePricing'
END as pricingType,

--Cannot mix types so need to split up into simple, then variable, and then merge
--BRING AYUSH THROUGH THIS..
CASE
	WHEN json_exists(defaultFulfillmentCosts, '$.baseCostPerOrder')  
	
	THEN
	[json_object(
  
  'fulFillmentBaseCostPerOrder', json_extract_string(defaultFulfillmentCosts, '$.baseCostPerOrder') ::DOUBLE, 
  'fulFillmentCostPerAdditionalProduct', json_extract_string(defaultFulfillmentCosts, '$.costPerAdditionalProduct') ::DOUBLE, 
  
  'packagingBaseCostPerOrder', json_extract_string(defaultFulfillmentCosts, '$.packagingBaseCostPerOrder') ::DOUBLE, 
  'packagingCostPerAdditionalProduct', json_extract_string(defaultFulfillmentCosts, '$.packagingCostPerAdditionalProduct') ::DOUBLE, 
  
  'minUnits', 0,
  'maxUnits', 9999999
  
  )] ::JSON[]	
END as simpleFulFillmentTransformation,


--Make video to explain what is happening
CASE
	WHEN json_exists(defaultFulfillmentCosts, '$.baseCostPerOrder') IS FALSE
	
	THEN 


  list_transform(
  defaultFulfillmentCosts :: JSON[],
  x -> json_object(
    
   'fulFillmentBaseCostPerOrder', json_extract_string(x, '$.fulFillmentBaseCostPerOrder') ::DOUBLE, 
  'fulFillmentCostPerAdditionalProduct', json_extract_string(x, '$.fulFillmentCostPerAdditionalProduct') ::DOUBLE, 
  
  'packagingBaseCostPerOrder', json_extract_string(x, '$.packagingBaseCostPerOrder') ::DOUBLE, 
  'packagingCostPerAdditionalProduct', json_extract_string(x, '$.packagingCostPerAdditionalProduct') ::DOUBLE, 
  
    'packagingBaseCostPerOrder', json_extract_string(x, '$.packagingBaseCostPerOrder') ::DOUBLE, 
    
  'minUnits', json_extract_string(x, '$.minUnits.id') ::INT, 
   'maxUnits', json_extract_string(x, '$.maxUnits.id') ::INT
  
    )
  )
END as variableFulFillmentTransformation,


--This merges the two values into a finaloutput array. (really it is just merging a null with an actual because it can never be both at the same time)
list_concat(simpleFulFillmentTransformation, variableFulFillmentTransformation) AS finalFulfillmentCost


FROM data_lake_defaultValues;


/*
*************************************************************************************************************************************
*************************************************************************************************************************************
********************* Step 3: WRITE TO OUTPUT TABLES											***************************************
*************************************************************************************************************************************
*************************************************************************************************************************************
*/

-- Step 3: output

COPY model_defaultValues TO '${s3BucketRoot}/tenants/tenantid=${tenantId}/transposed-output/tablename=lookups/model_defaultValues.parquet'
(FORMAT parquet, COMPRESSION zstd);
SELECT * FROM model_defaultValues;




