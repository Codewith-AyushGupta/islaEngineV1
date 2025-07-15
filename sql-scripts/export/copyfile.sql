COPY (SELECT 'TEST' AS test)
TO '${s3BucketRoot}/tenants/tenantid=${tenantId}/tablename=export-output/${queryname}_${uuid}.csv'
(FORMAT CSV, DELIMITER ',', HEADER);
