import json
import toml
from datetime import datetime

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



