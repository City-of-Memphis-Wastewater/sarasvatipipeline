import requests
from src.pipeline.calls import make_request, call_ping
from src.pipeline.env import find_urls

class RjnClient:
    def __init__(self,config):
        self.config = config

    def get_token_and_headers(self):
        print("\nRjnClient.get_token_and_headers()")
        request_url = self.config['url'] + 'auth'
        print(f"request_url = {request_url}")
        data = {
            'client_id': self.config['client_id'],
            'password': self.config['password']
        }
        try:
            response = make_request(request_url, data)
        except ConnectionError as e:
            print("Skipping RjnClient.get_token_and_headers() due to connection error")
            print(e)
            return None, None
        if response is None:
            return None, None
        token = response.json().get("token")

        ['sessionId']
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"  
        }
        return token, headers
    
    def send_point(self, payload: dict):
        request_url = self.api_url + 'data/point'  # Adjust this if needed
        response = make_request(url=request_url, headers=self.headers, data=payload)
        if response.status_code == 200:
            print(f"Successfully posted point {payload.get('rjn_name')}")
        else:
            print(f"Failed to post point {payload.get('rjn_name')}: {response.status_code}")

def send_data_to_rjn(base_url:str, project_id:str, entity_id:int, headers:dict, timestamps, values):
    if timestamps is None:
        raise ValueError("timestamps cannot be None")
    if values is None:
        raise ValueError("values cannot be None")
    if not isinstance(timestamps, list):
        timestamps = [timestamps]
    if not isinstance(values, list):
        values = [values]
    # Check for matching lengths of timestamps and values
    if len(timestamps) != len(values):
        raise ValueError(f"timestamps and values must have the same length: {len(timestamps)} vs {len(values)}")


    url = f"{base_url}/projects/{project_id}/entities/{entity_id}/data"
    params = {
        "interval": 300,
        "import_mode": "OverwriteExistingData",
        "incoming_time": "DST"
    }
    body = {
    "comments": "Imported from EDS.",
    "data": dict(zip(timestamps, values))  # Works for single or multiple entries
    }

    response = None
    try:
        response = make_request(url=url, headers=headers, params = params, method="POST", data=body)
        if response is None:
            print("Response = None, job cancelled")
        else:
            response.raise_for_status()
            #print(f"Sent {timestamps} -> {values} to entity {entity_id} (HTTP {response.status_code})")
            print(f"Sent timestamps and values to entity {entity_id} (HTTP {response.status_code})")
    except ConnectionError as e:
        print("Skipping RjnClient.send_data_to_rjn() due to connection error")
        print(e)
    except requests.exceptions.RequestException as e:
        print(f"Error sending data to RJN: {e}")
        if response is not None:# and response.status_code != 500:
            print(f"Response content: {response.text}")  # Print error response
        
def ping():
    from src.pipeline.env import SecretsYaml
    from src.pipeline.projectmanager import ProjectManager
    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    secrets_file_path = project_manager.get_configs_secrets_file_path()
    config_obj = SecretsYaml.load_config(secrets_file_path = secrets_file_path)
    url_set = find_urls(config_obj)
    for url in url_set:
        if "rjn" in url.lower():
            print(f"ping url: {url}")
            call_ping(url)


if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "default"

    if cmd == "ping":
        ping()
    else:
        print("Usage options: \n"
        "poetry run python -m pipeline.api.rjn ping")