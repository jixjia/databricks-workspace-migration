'''
Author:         Jixin Jia (Gin)
Created:        2020-9-5
Description:    This program migrates (downlaods) all contents from a desginated databricks workspace

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
    shard = config.SOURCE_WORKSPACE_URI + '/api/'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(config.SOURCE_PERSONAL_TOKEN)
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

# Download items to Workspace
def export_workspace(path):
    params = {
        'path': path,
        'format': 'DBC',
        'direct_download': 'true'
    }
    endpoint = '2.0/workspace/export/'
    return api(endpoint, 'GET', params, None)
    
    
if __name__ == "__main__":

    # List workspace items
    print(f'[INFO] Listing items in workspace {config.SOURCE_WORKSPACE_URI}{config.SOURCE_WORKSPACE_PATH}')
    r = list_workspace(config.SOURCE_WORKSPACE_PATH)
    
    if len(r) > 0:
        for i in r['objects']:
            print(i)


    # Download items
    count = 0
    for i in r['objects']:
        try:
            print(f'[INFO] Downloading {i["path"]}')
            t0 = time.time()
            
            r = export_workspace(i["path"])
            
            t1 = time.time()
            if r.status_code >= 200 and r.status_code < 300:
                open(os.path.join(config.SOURCE_LOCAL_DIR, i["path"].split('/')[-1]), 'wb').write(r.content)
                count += 1
                print(f'[INFO] Successfully downloaded {i["path"]} (elapsed: {round(t1-t0,0)} sec)')
            else:
                print(r.text)
        except Exception as e:
            print(f'[ERROR] Failed to download {i["path"]}', e)
            
            
            
    