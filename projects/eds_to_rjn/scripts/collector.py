#pipeline.collector.py
from datetime import datetime

from src.pipeline.helpers import round_time_to_nearest_five_minutes
from src.pipeline.api.eds import fetch_eds_data

import csv
def collect_live_values(csv_path, eds_api, eds_headers_maxson, eds_headers_stiles):
    data = []
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

            except KeyError as e:
                print(f"Missing expected column in CSV: {e}")
                continue
            except ValueError as e:
                print(f"Invalid data in row: {e}")
                continue

            if row["zd"] == "Maxson":
                eds_headers = eds_headers_maxson
            elif row["zd"] == "WWTF":
                eds_headers = eds_headers_stiles

            try:
                ts, value = fetch_eds_data(
                    eds_api=eds_api,
                    site=row["zd"],
                    sid=eds_sid,
                    shortdesc=shortdesc,
                    headers=eds_headers
                )
                if value is not None:
                    rounded_dt = round_time_to_nearest_five_minutes(datetime.fromtimestamp(ts))
                    data.append({
                        "timestamp": rounded_dt.isoformat(),
                        "sid": eds_sid,
                        "shortdesc": shortdesc,
                        "rjn_siteid": rjn_siteid,
                        "rjn_entityid": rjn_entityid,
                        "value": round(value, 2)
                    })
            except Exception as e:
                print(f"Error on row: {e}")
    return data

