# main.py (or __main__.py)
from datetime import datetime
import csv
import sys
from pathlib import Path
from requests import Session # if you aren'ty using this, you should be

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
from src.pipeline.api.eds import fetch_eds_data2

def main():
    #sketch_maxson()
    sketch_daemon_runner_main()

def sketch_daemon_runner_main():
    #from . import daemon_runner
    from projects.eds_to_rjn.scripts import daemon_runner
    daemon_runner.main()


def sketch_maxson():
    test_connection_to_internet()

    project_name = 'eds_to_rjn' # project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    secrets_dict = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    queries_manager = QueriesManager(project_manager)
    try:
        queries_file_path_list = queries_manager.get_default_query_file_paths_list() # use default identified by the default-queries.toml file
        print(f"Using query file: {queries_file_path_list}")
    except FileNotFoundError as e:
        print(f"Error: {e}")

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
"---"

def sketch_andstiles():
    test_connection_to_internet()

    project_name = 'eds_to_rjn' # project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    secrets_dict = SecretsYaml.load_config(secrets_file_path = project_manager.get_configs_secrets_file_path())
    queries_manager = QueriesManager(project_manager)
    try:
        queries_file_path_list = queries_manager.get_default_query_file_paths_list() # use default identified by the default-queries.toml file
        print(f"Using query file: {queries_file_path_list}")
    except FileNotFoundError as e:
        print(f"Error: {e}")

    eds = EdsClient(secrets_dict['eds_apis'])
    token_eds, headers_eds_stiles = eds.get_token_and_headers(zd="WWTF")
    eds_api, headers_eds_maxson, headers_eds_stiles = get_eds_tokens_and_headers_both(secrets_dict) # Stiles EDS needs to be configured to allow access on the 43084 port. Compare both servers.

    rjn_api, headers_rjn = get_rjn_tokens_and_headers(secrets_dict)
    eds_sites = ["Maxson", "WWTF"]
    headers_eds = [headers_eds_maxson, headers_eds_stiles]
    for i,eds_headers in enumerate(headers_eds):
        eds_site = eds_sites[i]
        for csv_file_path in queries_file_path_list:
            process_sites_and_send(csv_file_path, eds_api, eds_site = eds_site, eds_headers = eds_headers, rjn_base_url=rjn_api.config['url'], rjn_headers=headers_rjn)

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


def call_eds_stiles_get_points_live(eds, headers_eds_stiles):
    print(f"\neds.get_points_live():")
    eds.get_points_live(api_id = "WWTF", sid = 5392,shortdesc = "INFLUENT",headers = headers_eds_stiles) # I-5005A.UNIT1@NET1
    eds.get_points_live(api_id = "WWTF", sid = 3550,shortdesc = "EFFLUENT",headers = headers_eds_stiles) # FI-405/415.UNIT1@NET1

def process_sites_and_send(csv_path, eds_api, eds_site, eds_headers, rjn_base_url, rjn_headers):
    "Retrieve and send immediately, without intermediate storage"
    "Altnerative: projects.eds_to_rjn.scripts.daemon_runner"
    print(f"\nmain.process_sites_and_send()")
    print(f"csv_path = {csv_path}")
    
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            #print(f"\trow = {row}")
            
            # Skip empty rows (if all values in the row are empty or None)
            if not any(row.values()):
                print("Skipping empty row.")
                continue
            
            try:
                # Convert and validate values
                eds_sid = int(row["sid"]) if row["sid"] not in (None, '', '\t') else None
                shortdesc = row.get("shortdesc", "")
                
                # Validate rjn_siteid and rjn_entityid are not None or empty
                rjn_siteid = row["rjn_siteid"] if row.get("rjn_siteid") not in (None, '', '\t') else None
                rjn_entityid = row["rjn_entityid"] if row.get("rjn_entityid") not in (None, '', '\t') else None
                
                # Ensure the necessary data is present, otherwise skip the row
                if None in (eds_sid, rjn_siteid, rjn_entityid):
                    print(f"Skipping row due to missing required values: SID={eds_sid}, rjn_siteid={rjn_siteid}, rjn_entityid={rjn_entityid}")
                    continue
                if eds_site != row["zd"]:
                    print(f"Skipping row due to mismatches site ID / ZD values: eds_site={eds_site}, row['zd']={row['zd']}")
                    continue

            except KeyError as e:
                print(f"Missing expected column in CSV: {e}")
                continue
            except ValueError as e:
                print(f"Invalid data in row: {e}")
                continue

            try:
                # Fetch data from EDS
                ts, value = fetch_eds_data2(
                    eds_api=eds_api,
                    site=eds_site,
                    sid=eds_sid,
                    shortdesc=shortdesc,
                    headers=eds_headers
                )

                if value is None:
                    print(f"Skipping null value for SID {eds_sid}")
                    continue

                # Process timestamp
                dt = datetime.fromtimestamp(ts)
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
            except Exception as e:
                print(f"Error processing SID {eds_sid}: {e}")

if __name__ == "__main__":
    main()
    #sketch_andstiles()

