# main.py (or __main__.py)
from datetime import datetime
import csv
import sys
from pathlib import Path
from requests import Session # if you aren'ty using this, you should be

from pipeline.api import eds
from pipeline.api import rjn

# Add the root project path so that 'src' can be found
ROOT = Path(__file__).resolve().parents[2]  # pipeline/projects/eds_to_rjn/scripts -> pipeline
sys.path.insert(0, str(ROOT))

from src.pipeline.env import SecretsYaml
from src.pipeline.api.eds import EdsClient
from src.pipeline.api.rjn import RjnClient
from src.pipeline.calls import test_connection_to_internet
from src.pipeline.helpers import round_time_to_nearest_five_minutes
from src.pipeline.projectmanager import ProjectManager
from src.pipeline.queriesmanager import QueriesManager
from src.pipeline.api.rjn import send_data_to_rjn
from src.pipeline.api.eds import fetch_eds_data
#from projects.eds_to_rjn.scripts import collector
from ..code import collector, storage, aggregator, sanitizer
from src.pipeline.queriesmanager import load_query_rows_from_csv_files, group_queries_by_api_url

def main():
    one_sec_test = True
    if one_sec_test:
        sketch_maxson()
    else:
        sketch_daemon_runner_main()

def sketch_daemon_runner_main():
    #from . import daemon_runner
    from projects.eds_to_rjn.scripts import daemon_runner
    daemon_runner.main()


def sketch_maxson():
    test_connection_to_internet()

    project_name = 'eds_to_rjn' # project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    queries_manager = QueriesManager(project_manager)
    queries_file_path_list = queries_manager.get_default_query_file_paths_list() # use default identified by the default-queries.toml file
    queries_dictarray = load_query_rows_from_csv_files(queries_file_path_list)
    queries_defaultdictlist = group_queries_by_api_url(queries_dictarray)
    secrets_dict = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    sessions = {}

    session_maxson = eds.login_to_session(api_url = secrets_dict["eds_apis"]["Maxson"]["url"] ,username = secrets_dict["eds_apis"]["Maxson"]["username"], password = secrets_dict["eds_apis"]["Maxson"]["password"])
    session_maxson.custom_dict = secrets_dict["eds_apis"]["Maxson"]
    sessions.update({"Maxson":session_maxson})

    session_rjn = rjn.login_to_session(api_url = secrets_dict["contractor_apis"]["RJN"]["url"] ,client_id = secrets_dict["contractor_apis"]["RJN"]["client_id"], password = secrets_dict["contractor_apis"]["RJN"]["password"])
    session_rjn.custom_dict = secrets_dict["contractor_apis"]["RJN"]
    sessions.update({"RJN":session_rjn})

    '''
    eds_api, headers_eds_maxson = get_eds_maxson_token_and_headers(secrets_dict)
    #eds_api, headers_eds_maxson, headers_eds_stiles = get_eds_tokens_and_headers_both(secrets_dict) # Stiles EDS needs to be configured to allow access on the 43084 port. Compare both servers.
    headers_eds_stiles = None
    try:
        rjn_api, headers_rjn = get_rjn_tokens_and_headers(secrets_dict)
    except:
        rjn_api = None
        headers_rjn = None
    for csv_file_path in queries_file_path_list:
        process_sites_and_send(csv_file_path, eds_api, eds_site = "Maxson", eds_headers = headers_eds_maxson, rjn_base_url=rjn_api.config['url'], rjn_headers=headers_rjn)
        
    # This is inherently over coupled - the data to send should come back to the main script so that is can be associated with another session
    '''
    #for key, session in sessions.items():
    key = "Maxson"
    session = sessions[key] 

    queries_defaultdict = queries_defaultdictlist.get(key,[])
    # data_updated should probably be  nested dictionaries rather than flattened rows, with keys for discerning source (localquery vs EDS vs RJN)
    data_updated = collector.collect_live_values(session, queries_defaultdict) # This returns everything known plus everything recieved. It is glorious. It is complete. It is not sanitized.
    data_sanitized_for_printing = sanitizer.sanitize_data_for_printing(data_updated)
    data_sanitized_for_aggregated_storage = sanitizer.sanitize_data_for_aggregated_storage(data_updated)

    for row in data_sanitized_for_printing:
        EdsClient.print_point_info_row(row)

    print(f"queries_defaultdict = {queries_defaultdict}")
    print(f"data_updated = {data_updated}")

    # Process timestamp
    for row in data_updated:
        dt = datetime.fromtimestamp(row["ts"])
        rounded_dt = round_time_to_nearest_five_minutes(dt)
        timestamp = rounded_dt
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        # Send data to RJN
        send_data_to_rjn(
            base_url=rjn_base_url,
            project_id=rjn_siteid,
            entity_id=rjn_entityid,
            headers=rjn_headers,
            timestamps=[timestamp_str],
            values=[round(value, 2)]
        )

        send_data_to_rjn2(
            session_rjn,
            project_id=session_rjn.custom_dict["url"],
            entity_id=rjn_entityid,
            timestamps=[timestamp_str],
            values=[round(value, 2)]
        )

# all of this can be mitigated with requests.Session()
# Used, dameon_runner.py, as of 04 June 2025
def get_eds_tokens_and_headers_both(secrets_dict):
    print("eds_to_rjn.scripts.main.get_eds_tokens_and_headers_both()")
    # toml headings
    eds_api = EdsClient(secrets_dict['eds_apis']) # eats both Maxson and Stiles
    token_eds, headers_eds_maxson = eds_api.get_token_and_headers(zd="Maxson")
    token_eds, headers_eds_stiles = eds_api.get_token_and_headers(zd="WWTF")
    return eds_api, headers_eds_maxson, headers_eds_stiles

def get_eds_maxson_token_and_headers(secrets_dict):
    print("eds_to_rjn.scripts.main.get_eds_maxson_tokens_and_headers()")
    # toml headings
    eds = EdsClient(secrets_dict['eds_apis'])
    token_eds, headers_eds_maxson = eds.get_token_and_headers(zd="Maxson")
    return eds, headers_eds_maxson

def get_eds_stiles_token_and_headers(secrets_dict):
    print("eds_to_rjn.scripts.main.get_eds_stiles_tokens_and_headers()")
    # toml headings
    eds = EdsClient(secrets_dict['eds_apis'])
    token_eds, headers_eds_maxson = eds.get_token_and_headers(zd="WWTP")
    return eds, headers_eds_maxson

def get_rjn_tokens_and_headers(secrets_dict):
    print("eds_to_rjn.scripts.main.get_rjn_tokens_and_headers()")
    # toml headings
    rjn = RjnClient(secrets_dict['contractor_apis']['RJN'])
    token_rjn, headers_rjn = rjn.get_token_and_headers()
    #print(f"token_rjn = {token_rjn}")
    return rjn, headers_rjn

if __name__ == "__main__":
    main()
