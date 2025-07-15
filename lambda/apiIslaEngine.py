from fastapi import FastAPI, Request , Response
from fastapi.responses import JSONResponse
from mangum import Mangum
import re
import json
import uuid
import gzip
import pandas as pd
from utils.islaEngineUtility import islaEngineUtility
from datetime import datetime
import os

app = FastAPI()

@app.post("/v1/{folderPath}")
async def queryByName(request: Request, folderPath: str):
    try:
        islaEngineUtilityInstance = islaEngineUtility()
        print('start:', json.dumps(islaEngineUtilityInstance.status_log))
        
        event = await resolveRequest(request)
        islaEngineUtilityInstance.status_log['event'] = event
        
        useValidationResponse = validateUser(event['headers'])
        if useValidationResponse is not True:
            islaEngineUtilityInstance.status_log['status'] = 'Failure'
            islaEngineUtilityInstance.status_log['error_message'] = "Unauthorized or Missing TenantId"
            islaEngineUtilityInstance.status_log['end_time'] = datetime.now().isoformat()
            print('finish', json.dumps(islaEngineUtilityInstance.status_log))       
            
            return useValidationResponse
        
        response = {}
        islaEngineUtilityInstance.status_log['tenant_id'] = event['headers']['x-tenant-id']
        
        variables = {
            "s3BucketRoot": os.environ.get("STORE_HERO_S3_BUCKET_ROOT"),
            "tenantId": event['headers']['x-tenant-id']
        }
        variables.update(event['body']['variables'])
        
        preValidationMessage = islaEngineUtilityInstance.preValidation(event['body']['queryName'] , folderPath ,variables)
        print('preValidationMessage',preValidationMessage)
        if preValidationMessage:
            islaEngineUtilityInstance.status_log['status'] = 'Failure'
            islaEngineUtilityInstance.status_log['error_message'] = preValidationMessage
            islaEngineUtilityInstance.status_log['end_time'] = datetime.now().isoformat()
            
            print('finish', json.dumps(islaEngineUtilityInstance.status_log))       
            return JSONResponse(
            content={"message": preValidationMessage},
            status_code=400
            )
             
        response = islaEngineUtilityInstance.queryByName(
            folderPath,
            event['body']['queryName'],
            variables
        )
        responseWithDataWrap = {"data":response}
        responseData = buildSuccessResponse(responseWithDataWrap)
        islaEngineUtilityInstance.status_log['status'] = 'finish'
        islaEngineUtilityInstance.status_log['end_time'] = datetime.now().isoformat()

    except Exception as e:
        islaEngineUtilityInstance.status_log['status'] = 'Failure'
        islaEngineUtilityInstance.status_log['error_message'] = str(e)
        islaEngineUtilityInstance.status_log['end_time'] = datetime.now().isoformat()
        message = f"Something went wrong. Please share this Id with Veckler support: {islaEngineUtilityInstance.status_log['correlationId']}"
        print('error:', json.dumps(islaEngineUtilityInstance.status_log))
        
        return JSONResponse(
            content={"message": message},
            status_code=500
        )

    print('finish', json.dumps(islaEngineUtilityInstance.status_log))
    return responseData
    return JSONResponse(
        content=compressed_data,
        status_code=200,
        headers=headers
    )

@app.post("/v1/{folderPath}/generate-file-url")
async def generateFileURl(request: Request, folderPath: str):
    try:
        islaEngineUtilityInstance = islaEngineUtility()
        print('start:', json.dumps(islaEngineUtilityInstance.status_log))
        event = await resolveRequest(request)
        islaEngineUtilityInstance.status_log['event'] = event
        useValidationResponse = validateUser(event['headers'])
        if useValidationResponse is not True:
            islaEngineUtilityInstance.status_log['status'] = 'Failure'
            islaEngineUtilityInstance.status_log['error_message'] = "Unauthorized or Missing TenantId"
            islaEngineUtilityInstance.status_log['end_time'] = datetime.now().isoformat()
            
            print('finish', json.dumps(islaEngineUtilityInstance.status_log))       
            return useValidationResponse
        response = {}
        islaEngineUtilityInstance.status_log['tenant_id'] = event['headers']['x-tenant-id']
        outputFileId = str(uuid.uuid4())
        variables = {
            "s3BucketRoot": os.environ.get("STORE_HERO_S3_BUCKET_ROOT"),
            "tenantId": event['headers']['x-tenant-id'],
            "uuid":outputFileId,
            "queryname":event['body']['queryName']
        }
        variables.update(event['body']['variables'])
        
        preValidationMessage = islaEngineUtilityInstance.preValidation(event['body']['queryName'] , folderPath ,variables)
        print('variables',variables)
        print('preValidationMessage',preValidationMessage)
        if preValidationMessage:  
            islaEngineUtilityInstance.status_log['status'] = 'Failure'
            islaEngineUtilityInstance.status_log['error_message'] = preValidationMessage
            islaEngineUtilityInstance.status_log['end_time'] = datetime.now().isoformat()
            
            print('finish', json.dumps(islaEngineUtilityInstance.status_log))             
            return JSONResponse(
            content={"message": preValidationMessage},
            status_code=400
            )
             
        response = islaEngineUtilityInstance.generatePreSignedUrl(
            folderPath,
            event['body']['queryName'],
            variables
        )
        islaEngineUtilityInstance.status_log['status'] = 'finish'
        islaEngineUtilityInstance.status_log['end_time'] = datetime.now().isoformat()
        
    except Exception as e:
        islaEngineUtilityInstance.status_log['status'] = 'Failure'
        islaEngineUtilityInstance.status_log['error_message'] = str(e)
        islaEngineUtilityInstance.status_log['end_time'] = datetime.now().isoformat()
        message = f"Something went wrong. Please share this Id with Veckler support: {islaEngineUtilityInstance.status_log['correlationId']}"
        print('error:', json.dumps(islaEngineUtilityInstance.status_log))
        return JSONResponse(
            content={"message": message},
            status_code=500
        )

    print('finish', json.dumps(islaEngineUtilityInstance.status_log))
    return JSONResponse(
        content={"data":response},
        status_code=200
    )

@app.post("/v1/{folderPath}/async")
async def handleAsyncProcess(request: Request, folderPath: str):
    
    try:
        islaEngineUtilityInstance = islaEngineUtility()
        print('start:', json.dumps(islaEngineUtilityInstance.status_log))
        event = await resolveRequest(request)
        islaEngineUtilityInstance.status_log['event'] = event
        useValidationResponse = validateUser(event['headers'])
        if useValidationResponse is not True:
            islaEngineUtilityInstance.status_log['status'] = 'Failure'
            islaEngineUtilityInstance.status_log['error_message'] = "Unauthorized or Missing TenantId"
            islaEngineUtilityInstance.status_log['end_time'] = datetime.now().isoformat()
            
            print('finish', json.dumps(islaEngineUtilityInstance.status_log))       
            return useValidationResponse
        response = {}
        islaEngineUtilityInstance.status_log['tenant_id'] = event['headers']['x-tenant-id']
        variables = {
           "s3BucketRoot": os.environ.get("STORE_HERO_S3_BUCKET_ROOT"),
            "tenantId": event['headers']['x-tenant-id'],
            "queryname":event['body']['queryName'],
            "uuid":str(uuid.uuid4())
        }
        variables.update(event['body']['variables'])
        # print('variables',variables)
        preValidationMessage = islaEngineUtilityInstance.preValidation(event['body']['queryName'] , folderPath ,variables)
        
        if preValidationMessage: 
            islaEngineUtilityInstance.status_log['status'] = 'Failure'
            islaEngineUtilityInstance.status_log['error_message'] = preValidationMessage
            islaEngineUtilityInstance.status_log['end_time'] = datetime.now().isoformat()
            
            print('finish', json.dumps(islaEngineUtilityInstance.status_log))       
            return JSONResponse(
            content={"message": preValidationMessage},
            status_code=400
            )
             
        response = islaEngineUtilityInstance.handleAsyncProcess(
            folderPath,
            event['body']['queryName'],
            variables
        )
        islaEngineUtilityInstance.status_log['status'] = 'finish'
        islaEngineUtilityInstance.status_log['end_time'] = datetime.now().isoformat()
        
    except Exception as e:
        islaEngineUtilityInstance.status_log['status'] = 'Failure'
        islaEngineUtilityInstance.status_log['error_message'] = str(e)
        islaEngineUtilityInstance.status_log['end_time'] = datetime.now().isoformat()
        message = f"Something went wrong. Please share this Id with Veckler support: {islaEngineUtilityInstance.status_log['correlationId']}"
        print('error:', json.dumps(islaEngineUtilityInstance.status_log))
        return JSONResponse(
            content={"message": message},
            status_code=500
        )
    print('finish', json.dumps(islaEngineUtilityInstance.status_log))
    return JSONResponse(
        content={'message':'Process started.'},
        status_code=202
    )

async def resolveRequest(request: Request):
    body = await request.json()
    headers = dict(request.headers)
    url = str(request.url)
    return {
        "body": body,
        "headers": headers,
        "url": url
    }

def buildSuccessResponse(response_data):
    if os.environ.get("STORE_HERO_S3_BUCKET_ROOT")=='LOCAL':
        return response_data
    json_data = json.dumps(response_data)
    byte_data = json_data.encode('utf-8')
    compressed_data = gzip.compress(byte_data, compresslevel=9)

    headers = {
        "Content-Type": "application/json",
        "Content-Encoding": "gzip"
    }

    return Response(
        content=compressed_data,
        status_code=200,
        headers=headers,
        media_type="application/json"
    )


def buildErrorResponse(message: str, status_code: int = 500):
    return JSONResponse(
        content={"message": message},
        status_code=status_code
    )


def validateUser(headers):
    print('headers:', headers)
    expected_token = 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZW5hbnRJZCI6InRlc3QtdGVuYW50IiwidXNlcklkIjoiMTIzNDU2In0.dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk'
    auth_header = headers.get('authorization')
    tenant_id = headers.get('x-tenant-id')

    if not tenant_id:
        return buildErrorResponse("TenantId is required.", status_code=401)

    if not auth_header or auth_header != expected_token:
        return buildErrorResponse("Unauthorized.", status_code=401)

    return True

# ---------------------- AWS Lambda Handler ----------------------

handler = Mangum(app)
