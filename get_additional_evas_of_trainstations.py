from datetime import datetime
import os
import time
import requests
from xml.dom.minidom import parseString

# Retrieve the secret API key from the environment variable
api_key = os.getenv("API_KEY")
if not api_key:
    raise ValueError("No API Key provided!")

client_id = os.getenv("CLIENT_ID")
if not client_id:
    raise ValueError("No Client Id provided!")

headers = {
    "DB-Api-Key": api_key,
    "DB-Client-Id": client_id,
    "accept": "application/xml",
}

def get_station_meta(eva):
    formatted_url = f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/station/{eva}"
    response = requests.get(formatted_url, headers=headers)
    time.sleep(1 / 60)  # because there can be a maximum of 60 requests per minute
    if response.status_code != 200:
        print(f"Error {response.status_code}: {formatted_url}")
        return None, None, None
    dom = parseString(response.content)
    stations = dom.getElementsByTagName("station")
    if not stations:
        print(f"No station found for EVA: {eva}")
        return None, None, None

    plan_url = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/{eva}/{date}/{hour}"
    date_str_url = datetime.now().strftime("%Y-%m-%d").replace("-", "")[2:]
    formatted_plan_url = plan_url.format(eva=eva, date=date_str_url, hour=f"{datetime.now().hour:02}")

    plan_response = requests.get(formatted_plan_url, headers=headers)
    time.sleep(1 / 60)  # because there can be a maximum of 60 requests per minute
    if plan_response.status_code != 200:
        print(f"Error {plan_response.status_code}: {formatted_url}")
    
    station = stations[0]
    meta = station.getAttribute("meta")
    name = station.getAttribute("name")
    return name, meta, plan_response.content

# Read EVA numbers from the file
with open("eva_list.txt", "r") as eva_file:
    eva_list = [line.strip() for line in eva_file]

# Process each EVA number
eva_and_name_list = []
alternative_station_name_to_station_name = {}
for eva in eva_list:
    name, meta, plan_content = get_station_meta(eva)
    if not name:
        continue
    if len(plan_content) > 20:
        eva_and_name_list.append((eva, name))
    else:
        print(f"Error, this eva: {eva}, should have a plan timetable.")
    if meta:
        for meta_eva in meta.split('|'):
            meta_eva = f"0{meta_eva}"
            meta_name, _, meta_plan_content = get_station_meta(meta_eva)
            if not meta_name:
                continue
            if len(meta_plan_content) > 20:
                eva_and_name_list.append((meta_eva, name))
                if meta_name != name:
                    alternative_station_name_to_station_name[meta_name] = name
                

print("All evas:")
for eva, name in sorted(eva_and_name_list, key=lambda x: x[1]):
    print(name, eva)
print("\n\n\nalternative_station_name_to_station_name:")
print(alternative_station_name_to_station_name)