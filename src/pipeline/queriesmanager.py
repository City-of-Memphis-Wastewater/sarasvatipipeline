import os
import toml
from datetime import datetime
import json
import csv
from collections import defaultdict


from src.pipeline import helpers

'''
Goal:
Set up to use the most recent query:
use-most-recently-edited-query-file = true # while true, this will ignore the files variable list and instead use a single list of the most recent files
'''

class QueriesManager:
    def __init__(self, project_manager: object):
        if not project_manager:
            raise ValueError("project_manager must be provided and not None.")
        self.project_manager = project_manager

    def get_default_query_file_paths_list(self, filename=None):
        """
        Returns a list of query CSV file paths:
        - If "filename" is provided, use only that one. Expected source: argparse cli
        - Else, try to read default-queries.toml for a list.
        - Else, fallback to ['points.csv']
        """
        if filename:
            "Return a one-csv list"
            paths = [self.project_manager.get_queries_file_path(filename)]
        else:
            try:
                default_query_path = os.path.join(
                    self.project_manager.get_queries_dir(), 'default-queries.toml'
                )
                with open(default_query_path, 'r') as f:
                    query_config = toml.load(f)
                filenames = query_config['default-query']['files']
                if not isinstance(filenames, list):
                    raise ValueError("Expected a list under 'files' in default-queries.toml")
                paths = [self.project_manager.get_queries_file_path(fname) for fname in filenames]
            except Exception as e:
                print(f"Warning: {e}. Falling back to ['points.csv']")
                paths = [self.project_manager.get_queries_file_path('points.csv')]

        for path in paths:
            if not os.path.exists(path):
                raise FileNotFoundError(f"Query file not found: {path}")
        return paths
    
    def load_tracking(self):
        file_path = self.project_manager.get_timestamp_success_file_path()
        try:
            return helpers.load_json(file_path)
        except FileNotFoundError:
            return {}
        
    def save_tracking(self,data):
        file_path = self.project_manager.get_timestamp_success_file_path()
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def get_most_recent_successful_timestamp(self,api_id):
        data = self.load_tracking()
        if data == {}:
            # if no stored value is found, assume you will go back one hour
            from datetime import timedelta
            delta = int(helpers.round_time_to_nearest_five_minutes(timedelta(hours = 1).total_seconds()))
            starttime = helpers.get_now_time() - delta 
        else:
            # if a stored most-recent value is found, use it as the starttime for your a tabular trend request, etc.
            starttime = helpers.round_time_to_nearest_five_minutes(datetime.fromisoformat(data[api_id]["timestamps"]["last_success"]))
            starttime = int(starttime.timestamp())
        return starttime
    
    def update_success(self,api_id,success_time=None):
        # This should be called when data is definitely transmitted to the target API. 
        # A confirmation algorithm might be in order, like calling back the data and checking it against the original.
        data = self.load_tracking()
        if api_id not in data:
            data[api_id] = {"timestamps": {}}
        now = success_time or datetime.now().isoformat()
        data[api_id]["timestamps"]["last_success"] = now
        data[api_id]["timestamps"]["last_attempt"] = now
        self.save_tracking(data)

    def update_attempt(self,api_id):
        data = self.load_tracking()
        if api_id not in data:
            data[api_id] = {"timestamps": {}}
        now = datetime.now().isoformat()
        data[api_id]["timestamps"]["last_attempt"] = now

def load_query_rows_from_csv_files(csv_paths_list):
    queries_dictarray = []
    for csv_path in csv_paths_list:
        print(f"csv_path = {csv_path}")
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                queries_dictarray.append(row)
    return queries_dictarray

def group_queries_by_api_url(queries_array):
    queries_array_grouped = defaultdict(list)
    for row in queries_array:
        api_id = row['zd']
        queries_array_grouped[api_id].append(row)
    return queries_array_grouped

def cli_queriesmanager():
    import argparse
    from src.pipeline.projectmanager import ProjectManager
    from src.pipeline.queriesmanager import QueriesManager
    parser = argparse.ArgumentParser(description="Select CSV file for querying.")
    parser.add_argument(
        '--csv-file',
        type=str,
        default=None,
        help="Specify the CSV file to use for querying (default is points.csv)"
    )
    args = parser.parse_args()

    # Set up project manager
    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    queries_manager = QueriesManager(project_manager)

    try:
        # Get the query file path (either default or user-provided)
        query_file_path = queries_manager.get_default_query_file_paths_list(args.csv_file)
        print(f"Using query file: {query_file_path}")
        # Further processing with the query file...
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ ==  "__main__":
    cli_queriesmanager()



