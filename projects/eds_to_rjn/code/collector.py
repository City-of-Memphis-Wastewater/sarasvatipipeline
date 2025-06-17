#pipeline.collector.py
from datetime import datetime

from src.pipeline.helpers import round_time_to_nearest_five_minutes
from src.pipeline.api.eds import fetch_eds_data, EdsClient

def collect_live_values(session, queries_defaultdict):   
    data = []
    for row in queries_defaultdict:
        print(f"\trow = {row}")

        # Skip empty rows (if all values in the row are empty or None)
        if not any(row.values()):
            print("Skipping empty row.")
            continue
        
        # light validation - if you want to change keys, that could be cool
        required_keys = ["iess", "rjn_siteid", "rjn_entityid"]
        if any(k not in row for k in required_keys):
            raise ValueError(f"Row missing required keys: {row}")
        
        
        try:
            # Convert and validate values
            #eds_sid = int(row["sid"]) if row["sid"] not in (None, '', '\t') else None
            iess = str(row["iess"]) if row["iess"] not in (None, '', '\t') else None
            shortdesc = row.get("shortdesc", "") # manual entry in the query CSV row, to have a useful brief name
            
            # Validate rjn_siteid and rjn_entityid are not None or empty
            rjn_siteid = row["rjn_siteid"] if row.get("rjn_siteid") not in (None, '', '\t') else None
            rjn_entityid = row["rjn_entityid"] if row.get("rjn_entityid") not in (None, '', '\t') else None
            
            # Ensure the necessary data is present, otherwise skip the row
            if None in (iess, rjn_siteid, rjn_entityid):
                print(f"Skipping row due to missing required values: iess={iess}, rjn_siteid={rjn_siteid}, rjn_entityid={rjn_entityid}")
                continue

        except KeyError as e:
            print(f"Missing expected column in CSV: {e}")
            continue
        except ValueError as e:
            print(f"Invalid data in row: {e}")
            continue

        try:
            old_ways_embedded = False
            if old_ways_embedded:
                ts, value = fetch_eds_data(session,iess)
                print(f"ts = {ts}, value = {value}")
                if value is not None:
                    rounded_dt = round_time_to_nearest_five_minutes(datetime.fromtimestamp(ts)) # arguably not appropriate at this point. round at transmission
                    
                    data.append({
                        "timestamp": rounded_dt.isoformat(),
                        "iess": iess,
                        "shortdesc": shortdesc,
                        "rjn_siteid": rjn_siteid,
                        "rjn_entityid": rjn_entityid,
                        "value": round(value, 2)
                    })
                fetched = {"timestamp":ts,"value":value}
                row.update(fetched)
                print(f"\trow->fetched = {row}")
            else:
                point_data = EdsClient.get_points_live_mod(session, iess)
                
                #fetched = {"timestamp":ts,"value":value}
                row.update(point_data)
                print(f"\trow->fetched = {row}")

            data.append(row)
        except Exception as e:
            print(f"Error on row: {e}")
    return data

