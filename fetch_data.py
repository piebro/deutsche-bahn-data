import os
import time
import requests
from datetime import datetime
from xml.dom.minidom import parseString
from pathlib import Path

# Retrieve the secret API key from the environment variable
api_key = os.getenv('API_KEY')
if not api_key:
    raise ValueError("No API Key provided!")

client_id = os.getenv('CLIENT_ID')
if not client_id:
    raise ValueError("No Client Id provided!")

headers = {
    'DB-Api-Key': api_key,
    'DB-Client-Id': client_id,
    'accept': 'application/xml'
}

def save_api_data(formatted_url, save_path, prettify=True):
    response = requests.get(formatted_url, headers=headers)
    if response.status_code != 200:
        print(f"error {response.status_code}: {formatted_url}")
        return
    with (save_path).open("w") as f:
        if prettify:
            f.write(parseString(response.content).toprettyxml())
        else:
            f.write(parseString(response.content).toxml())
    time.sleep(1/60) # because the can be a maximum of 60 requests per minute

def main():
    plan_url = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/{eva}/{date}/{hour}"
    fchg_url = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/{eva}"

    date_str = datetime.now().strftime("%Y-%m-%d")
    date_str_url = date_str.replace("-", "")[2:]

    save_folder = Path("data") / date_str
    save_folder.mkdir(exist_ok=True)

    with Path("eva_name_list.txt").open("r") as f:
        eva_name_list = [line.split(",") for line in f.read().split("\n")]

    curent_hour = datetime.now().hour
    for eva, name in eva_name_list:
        formatted_fchg_url = fchg_url.format(eva=eva)
        save_api_data(formatted_fchg_url, save_folder / f"{eva}_fchg_{curent_hour:02}.xml", prettify=False)
    
    for eva, name in eva_name_list:
        for hour in range(curent_hour, curent_hour + 3):
            hour = hour % 24
            formatted_plan_url = plan_url.format(eva=eva, date=date_str_url, hour=f"{hour:02}")
            save_api_data(formatted_plan_url, save_folder / f"{eva}_plan_{hour:02}.xml")

    print("Done")

if __name__ == "__main__":
    main()
