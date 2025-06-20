from datetime import datetime, timedelta
import json
import logging
from urllib3.exceptions import HTTPError 
from requests.exceptions import RequestException
import requests
import sys
import time

from src.pipeline.calls import make_request, call_ping
from src.pipeline.env import find_urls
from src.pipeline import helpers
from src.pipeline.queriesmanager import load_query_rows_from_csv_files, group_queries_by_api_url
from pprint import pprint

# Configure logging (adjust level as needed)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

class EdsClient:
    def __init__(self,config):
        self.config = config
    
    @staticmethod
    def get_license(session,api_url:str):
        response = session.get(api_url + 'license', json={}, verify=False).json()
        pprint(response)
        return response

    @staticmethod
    def print_point_info_row(row):
        # use theis when unpacking after bulk retrieval, not when retrieving
        print(f'''iess:{row["iess"]}, dt:{datetime.fromtimestamp(row["ts"])}, un:{row["un"]}, av:{round(row["value"],2)}, {row["shortdesc"]}''')

    @staticmethod
    def get_points_live_mod(session, iess: str):
        # please make this session based rather than header based
        "Access live value of point from the EDS, based on zs/api_id value (i.e. Maxson, WWTF, Server)"
        api_url = str(session.custom_dict["url"]) 

        query = {
            'filters' : [{
            'iess': [iess],
            'tg' : [0, 1],
            }],
            'order' : ['iess']
            }
        response = session.post(api_url + 'points/query', json=query, verify=False).json()
        #print(f"response = {response}")
        
        if response is None:
            return None
        
        points_datas = response.get("points", [])
        if not points_datas:
            raise ValueError(f"No data returned for iess='{iess}': len(points) == 0")
        elif len(points_datas) != 1:
            raise ValueError(f"Expected exactly one point, got {len(points_datas)}")
        else:
            point_data = points_datas[0] # You expect exactly one point usually
            #print(f"point_data = {point_data}")
        return point_data  
    
    def get_tabular_mod(session, req_id, point_list):
        results = [[] for _ in range(len(point_list))]
        while True:
            api_url = session.custom_dict['url']
            response = session.get(f'{api_url}trend/tabular?id={req_id}', verify=False).json()
            for chunk in response:
                if chunk['status'] == 'TIMEOUT':
                    raise RuntimeError('timeout')

                for idx, samples in enumerate(chunk['items']):
                    results[idx] += samples
                    
                if chunk['status'] == 'LAST':
                    return results
    
    @staticmethod
    def get_points_export(session,iess_filter:str=''):
        api_url = session.custom_dict["url"]
        zd = session.custom_dict["zd"]
        order = 'iess'
        query = '?zd={}&iess={}&order={}'.format(zd, iess_filter, order)
        request_url = api_url + 'points/export' + query
        response = session.get(request_url, json={}, verify=False)
        #print(f"Status Code: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}, Body: {response.text[:500]}")
        decoded_str = response.text
        return decoded_str

    @staticmethod
    def save_points_export(decoded_str, export_file_path):
        lines = decoded_str.strip().splitlines()

        with open(export_file_path, "w", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")  # Save each line in the text file
                
def fetch_eds_data(session, iess):
    point_data = EdsClient.get_points_live_mod(session, iess)
    if point_data is None:
        raise ValueError(f"No live point returned for iess {iess}")
    ts = point_data["ts"]
    value = point_data["value"]
    return ts, value

def fetch_eds_data_row(session, iess):
    point_data = EdsClient.get_points_live_mod(session, iess)
    return point_data

def login_to_session(api_url, username, password):
    session = requests.Session()

    data = {'username': username, 'password': password, 'type': 'script'}
    response = session.post(api_url + 'login', json=data, verify=False).json()
    #print(f"response = {response}")
    session.headers['Authorization'] = 'Bearer ' + response['sessionId']
    return session

def create_tabular_request(session, api_url, starttime, endtime, points):
    data = {
        'period': {
            'from': starttime, 
            'till': endtime, # must be of type int, like: int(datetime(YYYY, MM, DD, HH).timestamp()),
        },

        'step': 300,
        'items': [{
            'pointId': {'iess': p},
            'shadePriority': 'DEFAULT',
            'function': 'AVG'
        } for p in points],
    }
    response = session.post(api_url + 'trend/tabular', json=data, verify=False).json()
    #print(f"response = {response}")
    return response['id']

def wait_for_request_execution_session(session, api_url, req_id):
    st = time.time()
    while True:
        time.sleep(1)
        res = session.get(f'{api_url}requests?id={req_id}', verify=False).json()
        status = res[str(req_id)]
        if status['status'] == 'FAILURE':
            raise RuntimeError('request [{}] failed: {}'.format(req_id, status['message']))
        elif status['status'] == 'SUCCESS':
            break
        elif status['status'] == 'EXECUTING':
            print('request [{}] progress: {:.2f}\n'.format(req_id, time.time() - st))

    print('request [{}] executed in: {:.3f} s\n'.format(req_id, time.time() - st))

def demo_get_trabular_trend():
    print("Start: demo_show_points_tabular_trend()")
    # typical opening, for discerning the project, the secrets files, the queries, and preparing for sessions.
    from src.pipeline.projectmanager import ProjectManager
    from src.pipeline.env import SecretsYaml
    from src.pipeline.queriesmanager import QueriesManager

    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    queries_manager = QueriesManager(project_manager)
    secrets_dict = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    sessions = {}

    session_maxson = login_to_session(api_url = secrets_dict["eds_apis"]["Maxson"]["url"] ,username = secrets_dict["eds_apis"]["Maxson"]["username"], password = secrets_dict["eds_apis"]["Maxson"]["password"])
    session_maxson.custom_dict = secrets_dict["eds_apis"]["Maxson"]
    sessions.update({"Maxson":session_maxson})
    if False:
        session_stiles = login_to_session(api_url = secrets_dict["eds_apis"]["WWTF"]["url"] ,username = secrets_dict["eds_apis"]["WWTF"]["username"], password = secrets_dict["eds_apis"]["WWTF"]["password"])
        session_stiles.custom_dict = secrets_dict["eds_apis"]["WWTF"]
        sessions.update({"WWTF":session_stiles})

    queries_file_path_list = queries_manager.get_default_query_file_paths_list() # use default identified by the default-queries.toml file
    queries_dictlist = load_query_rows_from_csv_files(queries_file_path_list)
    queries_defaultdictlist = group_queries_by_api_url(queries_dictlist)
    
    for key, session in sessions.items():
        # Discern which queries to use
        point_list = [row['iess'] for row in queries_defaultdictlist.get(key,[])]

        # Discern the time range to use
        starttime = queries_manager.get_most_recent_successful_timestamp(api_id=key)
        endtime = helpers.get_now_time()

        request_id = create_tabular_request(session, session.custom_dict["url"], starttime, endtime, points=point_list)
        wait_for_request_execution_session(session, session.custom_dict["url"], request_id)
        results = EdsClient.get_tabular_mod(session, request_id, point_list)
        session.post(session.custom_dict["url"] + 'logout', verify=False)
        #queries_manager.update_success(api_id=key) # not appropriate here in demo without successful transmission to 3rd party API

        for idx, iess in enumerate(point_list):
            print('\n{} samples:'.format(iess))
            for s in results[idx]:
                #print('{} {} {}'.format(datetime.fromtimestamp(s[0]), s[1], s[2]))
                print(s)

def demo_eds_save_point_export():
    print("Start demo_eds_save_point_export()")
    from src.pipeline.env import SecretsYaml
    from src.pipeline.projectmanager import ProjectManager
    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    secrets_dict = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    sessions = {}

    session_maxson = login_to_session(api_url = secrets_dict["eds_apis"]["Maxson"]["url"] ,username = secrets_dict["eds_apis"]["Maxson"]["username"], password = secrets_dict["eds_apis"]["Maxson"]["password"])
    session_maxson.custom_dict = secrets_dict["eds_apis"]["Maxson"]
    sessions.update({"Maxson":session_maxson})

    decoded_str = EdsClient.get_points_export(session_maxson)
    export_file_path = project_manager.get_exports_file_path(filename = 'export_eds_points_neo.txt')
    EdsClient.save_points_export(decoded_str, export_file_path = export_file_path)
    print(f"Export file saved to: \n{export_file_path}")

def demo_get_license():
    print("\ndemo_get_license()")
    # typical opening, for discerning the project, the secrets files, the queries, and preparing for sessions.
    from src.pipeline.projectmanager import ProjectManager
    from src.pipeline.env import SecretsYaml

    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    secrets_dict = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    sessions = {}

    session_maxson = login_to_session(api_url = secrets_dict["eds_apis"]["Maxson"]["url"] ,username = secrets_dict["eds_apis"]["Maxson"]["username"], password = secrets_dict["eds_apis"]["Maxson"]["password"])
    session_maxson.custom_dict = secrets_dict["eds_apis"]["Maxson"]
    sessions.update({"Maxson":session_maxson})
    
    response = EdsClient.get_license(session_maxson, api_url = session_maxson.custom_dict["url"])

    if False:
        session_stiles = login_to_session(api_url = secrets_dict["eds_apis"]["WWTF"]["url"] ,username = secrets_dict["eds_apis"]["WWTF"]["username"], password = secrets_dict["eds_apis"]["WWTF"]["password"])
        session_stiles.custom_dict = secrets_dict["eds_apis"]["WWTF"]
        sessions.update({"WWTF":session_stiles})

        response = EdsClient.get_license(session_stiles, api_url = session_stiles.custom_dict["url"])

def ping():
    from src.pipeline.env import SecretsYaml
    from src.pipeline.projectmanager import ProjectManager

    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    secrets_dict = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    url_set = find_urls(secrets_dict)
    for url in url_set:
        if "43084" in url or "43080" in url: # Expected REST or SOAP API ports for the EDS 
            print(f"ping url: {url}")
            call_ping(url)

if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "default"

    if cmd == "demo-points-export":
        demo_eds_save_point_export()
    elif cmd == "demo-trend":
        demo_get_trabular_trend()
    elif cmd == "ping":
        ping()
    elif cmd == "license":
        demo_get_license()
    else:
        print("Usage options: \n" 
        "poetry run python -m pipeline.api.eds demo-points-export \n"  
        "poetry run python -m pipeline.api.eds demo-trend \n"
        "poetry run python -m pipeline.api.eds ping \n"
        "poetry run python -m pipeline.api.eds license")
    