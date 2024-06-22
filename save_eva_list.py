import os
import time
import requests
from xml.dom.minidom import parseString

import pandas as pd
import pdfplumber

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


def extract_tables_to_df(pdf_path):
    all_tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                df = pd.DataFrame(table[1:], columns=table[0])
                all_tables.append(df)

    combined_df = pd.concat(all_tables, ignore_index=True)
    return combined_df


print("getting 'Bahnhof' names")
df = extract_tables_to_df("Stationspreisliste-2024-data.pdf")
print("getting the eva for each 'Bahnhof' name")

df["Preis-\nklasse"] = df["Preis-\nklasse"].astype(int)
train_station_names = list(df[df["Preis-\nklasse"] <= 2]["Bahnhof"])


def get_eva(station_name):
    formatted_url = f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/station/{station_name}"
    response = requests.get(formatted_url, headers=headers)
    time.sleep(1 / 60)  # because there can be a maximum of 60 requests per minute

    if response.status_code != 200:
        print(f"error {response.status_code}: {formatted_url}")
        return None

    dom = parseString(response.content)

    if len(dom.getElementsByTagName("station")) == 0:
        print(f"empty response for station name: {station_name}")
        return None

    station = dom.getElementsByTagName("station")[0]
    eva = station.getAttribute("eva")
    return eva


# Create or open the file to write EVA numbers
with open("eva_list.txt", "w") as eva_file:
    for train_station_name in train_station_names:
        train_station_name = train_station_name.replace(" (", "(")
        train_station_name = train_station_name.replace(") ", ")")
        eva = get_eva(train_station_name)
        if eva:
            eva_file.write(f"0{eva}\n")

print("EVA numbers have been saved to eva_list.txt")
