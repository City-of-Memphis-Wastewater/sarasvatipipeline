import json
import toml
from datetime import datetime
import inspect
import types
import os


def load_json(filepath):
    # Load JSON data from the file
    with open(filepath, 'r') as file:
        data = json.load(file)
    return data

def load_toml(filepath):
    # Load TOML data from the file
    with open(filepath, 'r') as f:
        dic_toml = toml.load(f)
    return dic_toml

def round_time_to_nearest_five_minutes(dt: datetime) -> datetime:
    #print(f"dt = {dt}")
    allowed_minutes = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
    # Find the largest allowed minute <= current minute
    rounded_minute = max(m for m in allowed_minutes if m <= dt.minute)
    return dt.replace(minute=rounded_minute, second=0, microsecond=0)

def get_now_time():
    nowtime = round_time_to_nearest_five_minutes(datetime.now())
    print(f"rounded nowtime = {nowtime}")
    nowtime =  int(nowtime.timestamp())+300
    return nowtime

def function_view(globals_passed=None):
    # Use the calling frame to get info about the *caller* module
    caller_frame = inspect.stack()[1].frame
    if globals_passed is None:
        globals_passed = caller_frame.f_globals

    # Get filename → basename only (e.g., 'calls.py')
    filename = os.path.basename(caller_frame.f_code.co_filename)

    print(f"Functions defined in {filename}:")

    for name, obj in list(globals_passed.items()):
        if isinstance(obj, types.FunctionType):
            if getattr(obj, "__module__", None) == globals_passed.get('__name__', ''):
                print(f"  {name}")
    print("\n")


if __name__ == "__main__":
    function_view()