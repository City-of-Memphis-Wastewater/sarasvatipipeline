import requests
import certifi
import platform
import subprocess
import sys
import time
import logging
from urllib.parse import urlparse
from urllib3.exceptions import NewConnectionError

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

def test_connection_to_internet():
    try:
        # call Cloudflare's CDN test site, because it is lite.
        response = requests.get("http://1.1.1.1", timeout = 5)
        print("You are connected to the internet.")
    except:
        print(f"It appears you are not connected to the internet.")
        sys.exit()

def make_request(url, data=None, params = None, method="POST", headers=None, retries=3, delay=2, timeout=10, verify_ssl=True):
    """Now defunct, converted to a requests.Session() paradigm."""
    default_headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    merged_headers = {**default_headers, **(headers or {})}
    #print(f"merged_headers = {merged_headers}")

    verify = certifi.where() if verify_ssl else False

    request_func = {
        "POST": requests.post,
        "GET": requests.get,
        "PUT": requests.put,
        "DELETE": requests.delete,
        "PATCH": requests.patch,
    }.get(method.upper())

    if not request_func:
        logging.error(f"Unsupported HTTP method: {method}")
        return None
        #raise ValueError(f"Unsupported HTTP method: {method}")
    try:
        response = request_func(
            url,
            json=data,
            params=params,
            headers=merged_headers,
            timeout=timeout,
            verify=verify
        )
        response.raise_for_status()
        return response
    except requests.exceptions.SSLError as e:
        #raise ConnectionError(f"SSL Error: {e}")
        logging.error(f"SSL Error: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        if response.status_code == 500:
            logging.error(f"HTTP 500 Error - Response content: {response.text}")
        elif response.status_code == 503 and retries > 0:
            logging.warning(f"Service unavailable (503). Retrying in {delay} seconds...")
            time.sleep(delay)
            #return make_request(url, data, retries - 1, delay * 2)  # Exponential backoff
            return make_request(url, data, params, method, headers, retries - 1, delay * 2, timeout, verify_ssl)
        elif response.status_code == 403:
            #raise PermissionError("Access denied (403). The server rejected your credentials or IP.")
            logging.error("Access denied (403). The server rejected your credentials or IP.")
        else:
            #raise RuntimeError(f"HTTP error: {response.status_code} {response.text}")
            logging.error(f"HTTP error: {response.status_code} {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        logging.warning(f"Request failed: {e}")
        return None  # Ensures calling functions properly handle failure
    except NewConnectionError as e:
        logging.warning("Request failed due to connection issues.")
        logging.debug(f"Detailed error: {e}", exc_info=False)  # Only logs full traceback if DEBUG level is set

def call_ping(url):
    parsed = urlparse(url)
    param = "-n" if platform.system().lower() == "windows" else "-c"
    command = ["ping", param, "1", parsed.hostname]
    return subprocess.call(command) == 0  # True if ping succeeds

if __name__ == "__main__":
    from pipeline.helpers import function_view
    function_view()