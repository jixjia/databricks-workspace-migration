'''
Author:         Jixin Jia (Gin)
Created:        2020-9-5
Description:    This program migrates (uploads) all contents to a desginated databricks workspace

Databricks Workspace API References
https://docs.databricks.com/dev-tools/api/latest/workspace.html (AWS)
'''

import requests
import json
import time
import config
import argparse 
import os
import base64

# Add parser
parser = argparse.ArgumentParser()
parser.add_argument('-w', '--worker_type', type=str, help='Enter Cluster ID (mandatory)')
args = parser.parse_args() 


def api(endpoint, method='GET', params=None, payload=None):

    # Construct API call
    shard = config.DESTINATION_WORKSPACE_URI + '/api/'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(config.DESTINATION_PERSONAL_TOKEN)
    }
    url = shard + endpoint

    # Restfull Call
    try:
        return requests.get(url, params=params, headers=headers) if method == 'GET' else requests.post(url, headers=headers, params=params, data=payload)

    except Exception as e:
        print('[ERROR]', e)

# List items in Workspace
def list_workspace(path):
    params = {'path': path}
    endpoint = '2.0/workspace/list/'

    r = api(endpoint, 'GET', params)
    
    if r.status_code >= 200 and r.status_code < 300:
        res = r.json()
        return res
    else:
        return None

# Import items to Workspace
def import_workspace(payload):
    endpoint = '2.0/workspace/import/'
    return api(endpoint, 'POST', None, payload)
    
    
if __name__ == "__main__":

    # Upload items
    files = os.listdir(config.DESTINATION_LOCAL_DIR)
    count = 0
    for i in files:
        if i.endswith('dbc'):            
            try:
                data = open(os.path.join(config.DESTINATION_LOCAL_DIR, i), 'rb').read()
                
                print(f'[INFO] Uploading {i}')
                t0 = time.time()
                
                # Encode bytes to base64 and decode to string with  utf-8
                payload = {
                    "path": config.DESTINATION_WORKSPACE_PATH + str(i).replace('.dbc',''),
                    "format": "DBC",
                    "language": "PYTHON",
                    "content" : base64.b64encode(data).decode('utf-8')
                }

                r = import_workspace(json.dumps(payload))
                
                t1 = time.time()
                if r.status_code >= 200 and r.status_code < 300:
                    print(f'[INFO] Successfully uplaoded {i} (elapsed: {round(t1-t0,0)} sec)')
                    count += 1
                else:
                    print(r.text)
            except Exception as e:
                print(f'[ERROR] Failed to read/upload {i}', e)
            
            
            
    # List workspace items
    print(f'[INFO] Listing items in workspace {config.DESTINATION_WORKSPACE_PATH}')
    r = list_workspace(config.DESTINATION_WORKSPACE_PATH)
    
    if len(r) > 0:
        for i in r['objects']:
            print(i)