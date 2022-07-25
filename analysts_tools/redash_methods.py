"""The goal of this script is to store the different ways to interact with
Redash via API"""
import requests
import json
import pandas as pd
import time



#This method import the query results, with the last cached data (i.e. last execution)
#The url param is (generally) the complete url of a fixed query to obtain its data in json format
def import_redash_query(url):
    response = requests.get(url)
    #Print status code of the request
    print(response.status_code)
    data = pd.DataFrame.from_dict(response.json()['query_result']['data']['rows'])
    return data

#This method performs an execution of the query, and then obtain the query data results
#The redash_url param is the base url, the query_id param indicates the specific query
#The api_key corresponds to the used API Key (personal,not public)
#If the query requests additional params, params should be added
def get_fresh_query_result(redash_url,query_id,api_key,params,pause):
    #Each query execution generates a job, from which we extract its status
    def poll_job(s,redash_url,job,pause):
    # TODO: add timeout
        while job['status'] not in (3, 4):
            response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
            job = response.json()['job']
            time.sleep(pause)

        if job['status'] == 3:
            return job['query_result_id']

        return None

    s=requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})
    payload=dict(max_age=0, parameters=params)
    response=s.post('{}/api/queries/{}/results'.format(redash_url,query_id),data=json.dumps(payload))

    if response.status_code!=200:
        raise Exception('Refresh failed.')

    result_id = poll_job(s,redash_url,response.json()['job'],pause)

    if result_id:
        response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url,query_id,result_id))
        if response.status_code!=200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')

    print(response.status_code)
    return pd.DataFrame.from_dict(response.json()['query_result']['data']['rows'])


#
