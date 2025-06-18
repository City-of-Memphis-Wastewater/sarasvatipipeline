#pipeline.collector.py
from datetime import datetime
import logging
logger = logging.getLogger(__name__)

from src.pipeline.helpers import round_time_to_nearest_five_minutes
from src.pipeline.api.eds import fetch_eds_data, EdsClient

def collect_live_values(session, queries_defaultdict):   
    data = []
    for row in queries_defaultdict:
        #print(f"\trow = {row}")
        # Skip empty rows (if all values in the row are empty or None)
        if not any(row.values()):
            print("Skipping empty row.")
            continue
        
        # light validation - if you want to change keys, that could be cool
        required_keys = ["iess", "rjn_siteid", "rjn_entityid"]
        if any(k not in row for k in required_keys):
            raise ValueError(f"Row missing required keys: {row}")
        
        try:
            # extract and validate iess value from CSV row before it is used to retrieve data
            iess = str(row["iess"]) if row["iess"] not in (None, '', '\t') else None
        except KeyError as e:
            print(f"Missing expected column in CSV: {e}")
            continue
        except ValueError as e:
            print(f"Invalid data in row: {e}")
            continue

        try:
            point_data = EdsClient.get_points_live_mod(session, iess)
            conflicts = set(row.keys()) & set(point_data.keys())
            if conflicts:
                logger.debug(f"Warning: key collision on {conflicts}, for iess = {iess}. This is expected.")
            '''
            Not the worst idea:
            Use nested structures
            Instead of flattening all keys into the same dict, keep fetched data as a sub-dictionary.
            In which case, the aggregate should be JSON (or TOML, whatever), not CSV.
            However, we have something that works. It is fine for now.
            '''
            row.update(point_data)
            data.append(row)
        except Exception as e:
            print(f"Error on row: {e}")
    return data

