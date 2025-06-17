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
            response = make_request(url = request_url, data=data)
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

    def print_point_info_row(self,point_data, shortdesc):
        print(f'''{shortdesc}, sid:{point_data["sid"]}, iess:{point_data["iess"]}, dt:{datetime.fromtimestamp(point_data["ts"])}, un:{point_data["un"]}. av:{round(point_data["value"],2)}''')

    #def get_points_live(self,api_id: str,sid: int,shortdesc : str="",headers = None):
    def get_points_live(self,api_id: str,iess: str,shortdesc : str="",headers = None):
    #def get_points_live(self,session, iess: str):
        # please make this session based rather than header based
        "Access live value of point from the EDS, based on zs/api_id value (i.e. Maxson, WWTF, Server)"
        print(f"\nEdsClient.get_points_live")
        api_url = str(self.config[api_id]["url"]) # api_id should only ever refer to the secrets.yaml file key
        request_url = api_url + 'points/query'
        print(f"request_url = {request_url}")
        query = {
            'filters' : [{
            #'sid': [sid], # test without
            'iess': [iess], # test with
            'tg' : [0, 1],
            }],
            'order' : ['iess']
            }

        response = make_request(url = request_url, headers=headers, data = query)
        if response is None:
            return None
        else:
            byte_string = response.content
            decoded_str = byte_string.decode('utf-8')
            data = json.loads(decoded_str) 
            #pprint(f"data={data}")
            points_datas = data.get("points", [])
            if not points_datas:
                #print(f"{shortdesc}, sid:{sid}, no data returned, len(points)==0")
                print(f"{shortdesc}, iess:{iess}, no data returned, len(points)==0")
            else:
                for point_data in points_datas:
                    self.print_point_info_row(point_data, shortdesc)
            return points_datas[0]  # You expect exactly one point usually

    def get_points_live_mod(self, session, iess: str):
        # please make this session based rather than header based
        "Access live value of point from the EDS, based on zs/api_id value (i.e. Maxson, WWTF, Server)"
        print(f"\nEdsClient.get_points_live")
        api_url = str(session.custom_dict["url"]) 
        request_url = api_url + 'points/query'
        print(f"request_url = {request_url}")
        query = {
            'filters' : [{
            #'sid': [sid], # test without
            'iess': [iess], # test with
            'tg' : [0, 1],
            }],
            'order' : ['iess']
            }

        response = make_request(url = request_url, headers=headers, data = query)
        if response is None:
            return None
        else:
            byte_string = response.content
            decoded_str = byte_string.decode('utf-8')
            data = json.loads(decoded_str) 
            #pprint(f"data={data}")
            points_datas = data.get("points", [])
            if not points_datas:
                print(f"{shortdesc}, sid:{sid}, no data returned, len(points)==0")
            else:
                for point_data in points_datas:
                    self.print_point_info_row(point_data, shortdesc)
            return points_datas[0]  # You expect exactly one point usually
    
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
        
    def get_tabular_trend(self,api_id: str="Maxson",sid: int=0,iess:str="M100FI.UNIT0@NET0", starttime :int=1744661000,endtime:int=1744661700,shortdesc : str="INF-DEFAULT",headers = None):
        "Based on EDS REST API Python Examples.pdf, pages 36-37."
        
        '''create_tabular_request '''
        api_url = str(self.config[api_id]["url"]) # api_id should only ever refer to the secrets.yaml file key
        
        "Initialize the query with a POST request" 
        request_url = api_url + 'trend/tabular'
        
        data = {
            'period' : {
            'from' : starttime,
            'till' : endtime
            },
            'step' : 150,
            'items' : [{
            'pointId' : {
            'sid' : sid,
            'iess' : iess
            },
            'shadePriority' : 'DEFAULT'
            }]
            }
        
        response = make_request(url = request_url, headers=headers, data = data, method="POST")
        byte_string = response.content
        decoded_str = byte_string.decode('utf-8')
        data = json.loads(decoded_str)
        request_id = data["id"]
 
        response_json = json.loads(response.content.decode('utf-8'))        
        request_id = response_json["id"]
        
        def wait_for_request_execution(headers, req_id, api_url):
            st = time.time()
            while True:
                time.sleep(1)
                #response = session.get(f'{api_url}requests?id={req_id}', verify=False).json()
                response = make_request(url = api_url, headers=headers, params = {id:req_id}, method="GET")
                print(f"response = {response}")
                
                response_json = json.loads(response.content.decode('utf-8'))        
                status = response_json[str(req_id)]
                if status['status'] == 'FAILURE':
                    raise RuntimeError('request [{}] failed: {}'.format(req_id, status['message']))
                elif status['status'] == 'SUCCESS':
                    break
                elif status['status'] == 'EXECUTING':
                    print('request [{}] progress: {:.2f}\n'.format(req_id, time.time() - st))

            print('request [{}] executed in: {:.3f} s\n'.format(req_id, time.time() - st))

        wait_for_request_execution(headers, req_id = request_id, api_url = api_url)

        #time.sleep(4)
        if response is None:
            print("Tabular trend request failed: Check your secrets.yaml file URL.")
            sys.exit()
        byte_string = response.content
        decoded_str = byte_string.decode('utf-8')
        data = json.loads(decoded_str)
        pprint(data)
        pprint(f"data={data}")
        request_id = data["id"] # query request_id, to reference an existing process, see page 36 of EDS REST API Python Examples.pdf.
        pprint(f"request_id={request_id}")

        # Prepare to retrieve tabular trend data 
        query = '?id={}'.format(request_id) # the API expects 'id', but I use 'request_id' where possible for rigor.
        #data = {'id': request_id} # already true
        request_url = api_url + 'trend/tabular' + query
        #request_url = api_url + 'events/read' + query
        print(f"request_url = {request_url}")

        # Delay request. First check the request_id (AKA request_id) to see status.
        response = make_request(url = request_url, headers=headers, method = "GET") # includes the query request_id in the url
        byte_string = response.content
        print(f"byte_string = {byte_string}")
        decoded_str = byte_string.decode('utf-8')
        print(f"Status: {response.status_code}")
        print(decoded_str[:500])  # Print just a slice

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
                
def fetch_eds_data(eds_api, site, iess, shortdesc, headers):
    point_data = eds_api.get_points_live(api_id=site, iess=iess, shortdesc=shortdesc, headers=headers)
    if point_data is None:
        raise ValueError(f"No live point returned for iess {iess}")
    ts = point_data["ts"]
    value = point_data["value"]
    return ts, value

def fetch_eds_data2(eds_api, site, iess, shortdesc, headers):
    point_data = eds_api.get_points_live(api_id=site, iess=iess, shortdesc=shortdesc, headers=headers)
    if point_data is None:
        raise ValueError(f"No live point returned for iess {iess}")
    ts = point_data["ts"]
    value = point_data["value"]
    return ts, value

def demo_get_tabular_trend():
    print("Start: demo_show_points_tabular_trend()")
    from src.pipeline.env import SecretsYaml
    from src.pipeline.projectmanager import ProjectManager
    from src.pipeline.api.eds import EdsClient
    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    secrets_dict = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    key0 = list(secrets_dict.keys())[0]
    key00 = list(secrets_dict[key0].keys())[0] # test whichever key is first in secrets.yaml
    eds = EdsClient(secrets_dict[key0])
    token_eds, headers_eds = eds.get_token_and_headers(zd=key00)
    eds.get_tabular_trend(api_id=key00,shortdesc="DEMO",headers = headers_eds)
    print(f"End: demo_show_points_tabular_trend()")


def login_to_session(api_url, username, password):
    session = requests.Session()

    data = {'username': username, 'password': password, 'type': 'script'}
    res = session.post(api_url + 'login', json=data, verify=False).json()
    session.headers['Authorization'] = 'Bearer ' + res['sessionId']
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
    res = session.post(api_url + 'trend/tabular', json=data, verify=False).json()
    print(f"res = {res}")
    return res['id']

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

    queries_dictarray = load_query_rows_from_csv_files(queries_manager.get_default_query_file_paths_list())
    queries_defaultdictlist = group_queries_by_api_url(queries_dictarray)
    
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
        #demo_get_tabular_trend()
        demo_get_tabular_trend_OvationSuggested()
    elif cmd == "ping":
        ping()
    else:
        print("Usage options: \n" 
        "poetry run python -m pipeline.api.eds demo-points-export \n"  
        "poetry run python -m pipeline.api.eds demo-trend \n"
        "poetry run python -m pipeline.api.eds ping")
    