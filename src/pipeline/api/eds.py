from datetime import datetime, timedelta
import json
import logging
from urllib3.exceptions import HTTPError 
from requests.exceptions import RequestException
import requests
import sys
import time
import csv

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

    def get_token_and_headers(self,zd="Maxson"):
        logging.info("EdsClient.get_token_and_headers()")

        try:
            plant_cfg = self.config[zd]
        except KeyError:
            logging.error(f"Unknown zd '{zd}'")
            raise ValueError(f"Unknown zd '{zd}'")

        request_url = plant_cfg['url'] + 'login'
        logging.info(f"Requesting login at {request_url}")
        #print(f"request_url = {request_url}")
        data = {
            'username': plant_cfg['username'],
            'password': plant_cfg['password'],
            'type': 'rest client'
        }
        try:
            response = make_request(url = request_url, data=data, method="POST")
            if response is None:
                logging.warning("Request failed—received NoneType response. Skipping token retrieval.")
                return None, None  # Prevent AttributeError
            response.raise_for_status()  # Ensure response is valid
        except (RequestException, HTTPError) as e:
            logging.warning("Skipping token retrieval due to connection error.")
            logging.warning("Your base URL might not be correctly set in secrets.yaml.")
            #logging.debug(e)  # Only logs full traceback if logging level is set to DEBUG
            return None, None
        
        token = response.json()['sessionId']
        headers = {'Authorization': f"Bearer {token}"} if token else None

        return token, headers

    def get_license(self,api_id:str,headers=None):
        plant_cfg = self.config[api_id]
        request_url = plant_cfg['url'] + 'license'
        response = make_request(url = request_url, headers=headers, method = "GET", data = {})
        
        pprint(response.__dict__)
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

                if chunk['status'] == 'LAST':
                    return results

                for idx, samples in enumerate(chunk['items']):
                    results[idx] += samples

    def get_points_export(self,api_id: str,sid: int=int(),iess:str=str(), starttime :int=int(),endtime:int=int(),shortdesc : str="",headers = None):
        "Success"
        api_url = str(self.config[api_id]["url"])
        zd = api_id
        iess = ''
        order = 'iess'
        query = '?zd={}&iess={}&order={}'.format(zd, iess, order)
        request_url = api_url + 'points/export' + query
        response = make_request(url = request_url, headers=headers, method="GET")
        if response is None:
            sys.exit() # this is better than overwriting a previous exprt with a blank export
        byte_string = response.content
        decoded_str = byte_string.decode('utf-8')
        return decoded_str

    def save_points_export(self,decoded_str, export_file_path):
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

def get_query_point_list(csv_path, api_id):
    print(f"csv_path = {csv_path}")
    point_list = list()
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Skip empty rows (if all values in the row are empty or None)
            if not any(row.values()):
                print("Skipping empty row.")
                continue
            #print(f"row = {row}")
            if row['zd']!=api_id:
                pass
                #print(f"api_id {api_id} =! row['zd'] {row['zd']} in the query CSV row.")
            else:
                # Convert and validate values
                point = row["iess"]
                point_list.append(point)
                
    return point_list

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

def demo_get_tabular_trend_OvationSuggested():
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
                print('{} {} {}'.format(datetime.fromtimestamp(s[0]), s[1], s[2]))

def demo_eds_save_point_export():
    print("Start demo_eds_save_point_export()")
    from src.pipeline.env import SecretsYaml
    from src.pipeline.projectmanager import ProjectManager
    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    secrets_dict = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    key0 = list(secrets_dict.keys())[0]
    key00 = list(secrets_dict[key0].keys())[0]
    eds = EdsClient(secrets_dict[key0])
    token_eds, headers_eds = eds.get_token_and_headers(zd=key00)
    decoded_str = eds.get_points_export(api_id = key00,headers = headers_eds)
    export_file_path = project_manager.get_exports_file_path(filename = 'export_eds_points_all.txt')
    eds.save_points_export(decoded_str, export_file_path = export_file_path)
    print(f"Export file will be saved to: {export_file_path}")

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
        demo_get_tabular_trend_OvationSuggested()
    elif cmd == "ping":
        ping()
    else:
        print("Usage options: \n" 
        "poetry run python -m pipeline.api.eds demo-points-export \n"  
        "poetry run python -m pipeline.api.eds demo-trend \n"
        "poetry run python -m pipeline.api.eds ping")
    