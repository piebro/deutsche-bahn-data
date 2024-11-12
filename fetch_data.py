import os
import time
import requests
from datetime import datetime
from xml.dom.minidom import parseString
from pathlib import Path
from requests.exceptions import RequestException
import pandas as pd

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


def save_api_data(formatted_url, save_path, prettify=True, max_retries=4):
    for attempt in range(max_retries):
        try:
            response = requests.get(formatted_url, headers=headers, timeout=10)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            if attempt > 0:
                print(f"Success after {attempt} attempts.")

            with save_path.open("w") as f:
                if prettify:
                    f.write(parseString(response.content).toprettyxml())
                else:
                    f.write(parseString(response.content).toxml())
            
            time.sleep(1 / 60)  # Rate limiting
            return  # Success, exit the function

        except (RequestException, ConnectionError) as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print("Retrying in 1 seconds...")
                time.sleep(1)
            else:
                print(f"Failed to fetch data after {max_retries} attempts: {formatted_url}")

    print(f"error: Could not retrieve data for {formatted_url}")


def main():
    plan_url = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/{eva}/{date}/{hour}"
    fchg_url = ("https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/{eva}")

    date_str = datetime.now().strftime("%Y-%m-%d")
    date_str_url = date_str.replace("-", "")[2:]

    save_folder = Path("data") / date_str
    save_folder.mkdir(exist_ok=True, parents=True)

    df = pd.read_csv("eva_list.csv")
    eva_list = []
    for evas in df['evas']:
        eva_list.extend(evas.split(","))

    curent_hour = datetime.now().hour
    for eva in eva_list:
        formatted_fchg_url = fchg_url.format(eva=eva)
        save_api_data(
            formatted_fchg_url,
            save_folder / f"{eva}_fchg_{curent_hour:02}.xml",
            prettify=False,
        )

    print("curent_hour:", curent_hour)
    for eva in eva_list:
        for hour in range(curent_hour, curent_hour + 6):  # fetch this hour and the next 5 hours
            hour = hour % 24
            formatted_plan_url = plan_url.format(
                eva=eva, date=date_str_url, hour=f"{hour:02}"
            )
            save_api_data(formatted_plan_url, save_folder / f"{eva}_plan_{hour:02}.xml")

    print("Done")

if __name__ == "__main__":
    main()
