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
from src.pipeline.api.rjn import send_data_to_rjn2
from src.pipeline.api.eds import fetch_eds_data
#from projects.eds_to_rjn.scripts import collector
from ..code import collector, storage, aggregator, sanitizer
from src.pipeline.queriesmanager import load_query_rows_from_csv_files, group_queries_by_api_url


import logging

logging.basicConfig(level=logging.DEBUG)  # or INFO, WARNING, ERROR

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)  

def main():
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
    logger.debug(f"queries_file_path_list = {queries_file_path_list}")
    queries_dictlist = load_query_rows_from_csv_files(queries_file_path_list)
    queries_defaultdictlist = group_queries_by_api_url(queries_dictlist)
    secrets_dict = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    sessions = {}

    session_maxson = eds.login_to_session(api_url = secrets_dict["eds_apis"]["Maxson"]["url"] ,username = secrets_dict["eds_apis"]["Maxson"]["username"], password = secrets_dict["eds_apis"]["Maxson"]["password"])
    session_maxson.custom_dict = secrets_dict["eds_apis"]["Maxson"]
    sessions.update({"Maxson":session_maxson})
    
    session_rjn = rjn.login_to_session(api_url = secrets_dict["contractor_apis"]["RJN"]["url"] ,client_id = secrets_dict["contractor_apis"]["RJN"]["client_id"], password = secrets_dict["contractor_apis"]["RJN"]["password"])
    session_rjn.custom_dict = secrets_dict["contractor_apis"]["RJN"]
    sessions.update({"RJN":session_rjn})

    #for key, session in sessions.items():
    key = "Maxson"
    session = sessions[key] 

    queries_defaultdict = queries_defaultdictlist.get(key,[])
    # data_updated should probably be  nested dictionaries rather than flattened rows, with keys for discerning source (localquery vs EDS vs RJN)
    data_updated = collector.collect_live_values(session, queries_defaultdict) # This returns everything known plus everything recieved. It is glorious. It is complete. It is not sanitized.
    data_sanitized_for_printing = sanitizer.sanitize_data_for_printing(data_updated)
    data_sanitized_for_aggregated_storage = sanitizer.sanitize_data_for_aggregated_storage(data_updated)

    for row in data_sanitized_for_aggregated_storage:
        EdsClient.print_point_info_row(row)

        #print(f"queries_defaultdict = {queries_defaultdict}")
        #print(f"data_updated = {data_updated}")

        # Process timestamp
        
        #for row in data_updated:
        dt = datetime.fromtimestamp(row["ts"])
        rounded_dt = round_time_to_nearest_five_minutes(dt)
        timestamp = rounded_dt
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
        # Send data to RJN
        
        send_data_to_rjn2(
            session_rjn,
            base_url = session_rjn.custom_dict["url"],
            project_id=row["rjn_siteid"],
            entity_id=row["rjn_entityid"],
            timestamps=[timestamp_str],
            values=[round(row["value"], 2)]
        )


def get_rjn_tokens_and_headers(secrets_dict):
    print("eds_to_rjn.scripts.main.get_rjn_tokens_and_headers()")
    # toml headings
    rjn = RjnClient(secrets_dict['contractor_apis']['RJN'])
    token_rjn, headers_rjn = rjn.get_token_and_headers()
    #print(f"token_rjn = {token_rjn}")
    return rjn, headers_rjn

if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "default"

    if cmd == "sketch":
        sketch_maxson()
    elif cmd == "daemon_runner":
        sketch_daemon_runner_main()
    else:
        print("Usage options: \n"
        "poetry run python -m projects.eds_to_rjn.scripts.main daemon_runner \n"
        "poetry run python -m projects.eds_to_rjn.scripts.main sketch")
