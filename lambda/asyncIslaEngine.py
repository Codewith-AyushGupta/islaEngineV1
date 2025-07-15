import json
from utils.islaEngineUtility import islaEngineUtility
from datetime import datetime

def handler(event=None, context=None):
    try:
        islaEngineUtilityInstance = islaEngineUtility()
        print('start',json.dumps(islaEngineUtilityInstance))
        islaEngineUtilityInstance.status_log['event'] = event
        
        record =  event['Records'][0]
        raw_body = record['body']
        body = json.loads(raw_body)
        
        islaEngineUtilityInstance.status_log['tenant_id'] =  body.get('tenantId')
        
        tenant_id = body.get('tenantId')
        queryByName = body.get('queryName', None)
        path = body.get('path')
        variables = body.get('variables', {})
        
        response = islaEngineUtilityInstance.queryByName(
            path,
            queryByName,
            variables
        )
        
        islaEngineUtilityInstance.status_log['end_time'] =  datetime.now().isoformat()
        islaEngineUtilityInstance.status_log['status'] =  'Success'
        print('end',json.dumps(islaEngineUtilityInstance.status_log))
        
    except Exception as e:
        islaEngineUtilityInstance.status_log['end_time'] =  datetime.now().isoformat()
        islaEngineUtilityInstance.status_log['error_message'] =  str(e)
        islaEngineUtilityInstance.status_log['status'] =  'Failure'
        print('error', json.dumps(islaEngineUtilityInstance.status_log))
        
handler()
