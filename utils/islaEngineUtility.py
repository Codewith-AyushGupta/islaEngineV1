from datetime import datetime
import uuid
import re
import boto3
import os
import json
import duckdb

s3_client = boto3.client('s3')
sqs = boto3.client('sqs')
class islaEngineUtility:
    
    def __init__(self):
        self.status_log = {
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "tenant_id": None,
            "event": None,
            "status": "Started",
            "error_message": None,
            "correlationId": str(uuid.uuid4())
        }

    def queryByName(self, folderPath, queryName,variables):
        with open(f"./sql-scripts/{folderPath}/{queryName}.sql", "r", encoding="utf-8") as f:
            query = f.read()
            
        query = self.replaceVariables(query, variables)
        conn = self.duckDb()
        df = conn.execute(query).fetchdf()
        return df.to_dict(orient="records")
    
    def generatePreSignedUrl(self, folderPath, queryName, variables):
        tenant_id = self.status_log["tenant_id"]
        key = f"tenants/tenantid={tenant_id}/tablename=export-output/{queryName}_{variables['uuid']}.csv"
        print('Key',key)
        print('Variables',variables)
        print('bucketName',os.environ.get("STORE_HERO_BUCKET_NAME_ONLY"))
        
        with open(f"./sql-scripts/{folderPath}/{queryName}.sql", "r", encoding="utf-8") as f:
            query = f.read()
       
        query = self.replaceVariables(query, variables)
        
        conn = self.duckDb()
        df = conn.execute(query).fetchdf()
        preSignedUrl = self.generatePreSignedURL(os.environ.get("STORE_HERO_BUCKET_NAME_ONLY") ,key)
        return preSignedUrl

    def handleAsyncProcess(self, folderPath, queryName, variables):
        tenant_id = self.status_log["tenant_id"]
        with open(f"./sql-scripts/{folderPath}/{queryName}.sql", "r", encoding="utf-8") as f:
                query = f.read()
        query = self.replaceVariables(query, variables)
        message_body = {
        "queryByName": queryName,
        "path": folderPath,
        "variables": variables,
        "tenantId":tenant_id
        }
        queueUrl = os.environ.get("ISLA_ENGINE_QUEUE")
        print('message_body',message_body)
        print('message_body',queueUrl)
        params = {
            "QueueUrl":  queueUrl,
            "MessageBody": json.dumps(message_body),
            "MessageGroupId":f"{uuid.uuid4()}-{tenant_id}" ,   
            "MessageDeduplicationId":f"{uuid.uuid4()}-{tenant_id}"    
        }
        
        response = sqs.send_message(**params)
        return 
                                        
    def replaceVariables(self, query, variables):
        query = self.replaceIncludeFilesWithActualSQL(query)
        for key, value in variables.items():
            query = query.replace(f"${{{key}}}", str(value))
        return query

    def findMissingVariables(self, query):
        return set(re.findall(r"\$\{(.*?)\}", query))

    def preValidation(self , queryName ,folderPath, variables):
        
        with open(f"./sql-scripts/{folderPath}/{queryName}.sql", "r", encoding="utf-8") as f:
                query = f.read()
        query = self.replaceVariables(query,variables)
        remains =self.findMissingVariables(query)
        if len(remains)>0:
            return f"Missing required variables: {', '.join(remains)}"
        else:
            return False
    
    def duckDb(self):
        conn = duckdb.connect()
        print("2. Connected to DuckDB")

        # Load DuckDB S3 and httpfs extensions
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("INSTALL aws;")
        conn.execute("LOAD aws;")

        print("CREATE OR REPLACE SECRET secret Started.")
        print('Region',{os.environ.get("REGION")})
        conn.execute(f"""
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER credential_chain,
                REFRESH auto,
                REGION '{os.environ.get("REGION")}'
            );
        """)
        return conn

    def generatePreSignedURL(self,bucket_name, object_key, expiration=3600):
        response = s3_client.generatePreSignedURL(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expiration
        )
        return response
    
    def replaceIncludeFilesWithActualSQL(self, query):
        include_files = re.findall(r'@include\s+([^\s]+)', query)
        for include_file in include_files:
            include_file = include_file.strip('"').strip("'")
            
            start_key = include_file.find('sql-script')
            end_key = include_file.find('.sql')
            
            cleaned_path = include_file[start_key:end_key] + '.sql'
            cleaned_path = os.path.normpath(cleaned_path)
            
            with open(cleaned_path, "r", encoding="utf-8") as f:
                included_query = f.read()
            print('included_query',included_query)
            included_query = self.replaceIncludeFilesWithActualSQL(included_query)
            query = re.sub(rf'@include\s+{re.escape(include_file)}', included_query, query, count=1)

        return query
