from datetime import datetime
import json
import logging
from urllib3.exceptions import HTTPError 
from requests.exceptions import RequestException
import requests
import sys
import csv
import time

from src.pipeline.calls import make_request, call_ping
from src.pipeline.env import find_urls
from src.example.webapi_rest_get_tabular_trend_custom_config import get_tabular
from pprint import pprint

# Configure logging (adjust level as needed)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


class EdsClient:
    def __init__(self,config):
        self.config = config

    def get_token_and_headers(self,plant_zd="Maxson"):
        #print("\nEdsClient.get_token_and_headers()")
        logging.info("EdsClient.get_token_and_headers()")

        try:
            plant_cfg = self.config[plant_zd]
        except KeyError:
            logging.error(f"Unknown plant_zd '{plant_zd}'")
            raise ValueError(f"Unknown plant_zd '{plant_zd}'")

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

    def get_license(self,site:str,headers=None):
        plant_cfg = self.config[site]
        request_url = plant_cfg['url'] + 'license'
        response = make_request(url = request_url, headers=headers, method = "GET", data = {})
        pprint(response.__dict__)
        return response

    def print_point_info_row(self,point_data, shortdesc):
        print(f'''{shortdesc}, sid:{point_data["sid"]}, iess:{point_data["iess"]}, dt:{datetime.fromtimestamp(point_data["ts"])}, un:{point_data["un"]}. av:{round(point_data["value"],2)}''')


    def get_points_live(self,site: str,sid: int,shortdesc : str="",headers = None):
        "Access live value of point from the EDS, based on zs/site value (i.e. Maxson, WWTF, Server)"
        print(f"\nEdsClient.get_points_live")
        api_url = str(self.config[site]["url"])
        request_url = api_url + 'points/query'
        print(f"request_url = {request_url}")
        query = {
            'filters' : [{
            #'zd' : ['Maxson','WWTF','Server','Default'], # What is the default EDS zd name? 
            'sid': [sid],
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
        
    def get_tabular_trend(self,site: str="Maxson",sid: int=0,iess:str="M100FI.UNIT0@NET0", starttime :int=1744661000,endtime:int=1744661700,shortdesc : str="INF-DEFAULT",headers = None):
        "Based on EDS REST API Python Examples.pdf, pages 36-37."
        
        '''create_tabular_request '''
        api_url = str(self.config[site]["url"])
        
        "Initialize the query with a POST request" 
        request_url = api_url + 'trend/tabular'
        
        data = {
            'period' : {
            'from' : starttime,
            'till' : endtime
            },
            'step' : 1,
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
                print(f"dir(response) = {dir(response)}")
                print(f"response.__dict__ = {response.__dict__}")
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

    def get_points_export(self,site: str,sid: int=int(),iess:str=str(), starttime :int=int(),endtime:int=int(),shortdesc : str="",headers = None):
        "Success"
        api_url = str(self.config[site]["url"])
        zd = site
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

def fetch_eds_data(eds_api, site, sid, shortdesc, headers):
    point_data = eds_api.get_points_live(site=site, sid=sid, shortdesc=shortdesc, headers=headers)
    if point_data is None:
        raise ValueError(f"No live point returned for SID {sid}")
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
    config_obj = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    key0 = list(config_obj.keys())[0]
    key00 = list(config_obj[key0].keys())[0] # test whichever key is first in secrets.yaml
    eds = EdsClient(config_obj[key0])
    token_eds, headers_eds = eds.get_token_and_headers(plant_zd=key00)
    eds.get_tabular_trend(site=key00,shortdesc="DEMO",headers = headers_eds)
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
            'from': starttime, # int(datetime(2024, 12, 16, 15).timestamp()),
            'till': endtime, # int(datetime(2024, 12, 16, 18).timestamp()),
        },
        'step': 600,
        'items': [{
            'pointId': {'iess': p},
            'shadePriority': 'DEFAULT',
            'function': 'AVG'
        } for p in points],
    }
    res = session.post(api_url + 'trend/tabular', json=data, verify=False).json()
    print(f"res = {res}")
    #print(f"res.__dict__ = {res.__dict__}")
    return res['id']

def get_query_point_list(csv_path, unqiue_id):
    print(f"csv_path = {csv_path}")
    point_list = list()
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            #print(f"\trow = {row}")
            
            # Skip empty rows (if all values in the row are empty or None)
            if not any(row.values()):
                print("Skipping empty row.")
                continue
            #print(f"row = {row}")
            
            if ('iess' in row.keys() and row['zd']==unqiue_id):
                # Convert and validate values
                point = row["iess"]
                point_list.append(point)
            else:
                print("no iess values found in row, skipping")
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
    from src.pipeline.env import SecretsYaml
    from src.pipeline.projectmanager import ProjectManager
    from src.pipeline.queriesmanager import QueriesManager
    from src.pipeline.api.eds import EdsClient
    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    queries_manager = QueriesManager(project_manager)
    queries_file_path_list = queries_manager.get_query_file_paths() # use default identified by the default-queries.toml file
    config_obj = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())

    #eds = EdsClient(config_obj["eds_apis"]) # this design is defunct - use session 
    # benefits: more modular, more explicit, less exposed
    # there can be an EdsClient class, to remember the session instance and to know the necessary methods 

    point_list = list()
    for csv_file_path in queries_file_path_list:
        point_list.extend(get_query_point_list(csv_file_path, unqiue_id = "Maxson"))
    print(f"point_list = {point_list}")

    session_maxson = login_to_session(api_url = config_obj["eds_apis"]["Maxson"]["url"] ,username = config_obj["eds_apis"]["Maxson"]["username"], password = config_obj["eds_apis"]["Maxson"]["password"])
    #session_stiles = login_to_session(api_url = config_obj["eds_apis"]["WWTF"]["url"] ,username = config_obj["eds_apis"]["WWTF"]["username"], password = config_obj["eds_apis"]["WWTF"]["password"])
    
    starttime = int(datetime(2024, 12, 16, 15).timestamp())
    endtime = int(datetime(2024, 12, 16, 18).timestamp())
    
    request_id = create_tabular_request(session_maxson, config_obj["eds_apis"]["Maxson"]["url"], starttime, endtime, points=point_list)
    wait_for_request_execution_session(session_maxson, config_obj["eds_apis"]["Maxson"]["url"], request_id)
    results = get_tabular(session_maxson, request_id)

    session_maxson.post(config_obj["eds_apis"]["Maxson"]["url"] + 'logout', verify=False)

    #request_id = create_tabular_request(session_stiles, config_obj["eds_apis"]["WWTF"]["url"], starttime, endtime, points=point_list)
    
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
    config_obj = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    key0 = list(config_obj.keys())[0]
    key00 = list(config_obj[key0].keys())[0]
    eds = EdsClient(config_obj[key0])
    token_eds, headers_eds = eds.get_token_and_headers(plant_zd=key00)
    decoded_str = eds.get_points_export(site = key00,headers = headers_eds)
    export_file_path = project_manager.get_exports_file_path(filename = 'export_eds_points_all.txt')
    eds.save_points_export(decoded_str, export_file_path = export_file_path)
    print(f"Export file will be saved to: {export_file_path}")

def ping():
    from src.pipeline.env import SecretsYaml
    from src.pipeline.projectmanager import ProjectManager
    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    config_obj = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    url_set = find_urls(config_obj)
    for url in url_set:
        if "43084" in url or "43080" in url: # Expected REST or SOAP API ports for the EDS 
            print(f"ping url: {url}")
            call_ping(url)

if __name__ == "__main__":
    #demo_eds_save_point_export()
    #demo_get_tabular_trend()
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
    