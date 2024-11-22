import json
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm


def get_plan_xml_rows(xml_path, alternative_station_names):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    station = root.get("station")
    if station in alternative_station_names:
        station = alternative_station_names[station]

    rows = []
    for s in root.findall("s"):
        s_id = s.get("id")
        train_type = s.find("tl").get("c") if s.find("tl") is not None else None
        train_number = s.find("tl").get("n") if s.find("tl") is not None else None
        ar_train_line_number = s.find("ar").get("l") if s.find("ar") is not None else None
        dp_train_line_number = s.find("dp").get("l") if s.find("dp") is not None else None

        if train_type in ["IC", "ICE", "EC"]:
            train_name = f"{train_type} {train_number}"
        else:
            if ar_train_line_number is not None:
                train_name = f"{train_type} {ar_train_line_number}"
            elif dp_train_line_number is not None:
                train_name = f"{train_type} {dp_train_line_number}"
            else:
                train_name = train_type

        s_id_split = s_id.split("-")

        dp_ppth = s.find("dp").get("ppth") if s.find("dp") is not None else None  # departure planed path
        if dp_ppth is None:
            final_destination_station = station
        else:
            final_destination_station = dp_ppth.split("|")[-1]

        rows.append(
            {
                "id": s_id,
                "station": station,
                "train_name": train_name,
                "final_destination_station": final_destination_station,
                "train_type": train_type,
                "arrival_planned_time": s.find("ar").get("pt") if s.find("ar") is not None else None,
                "departure_planned_time": s.find("dp").get("pt") if s.find("dp") is not None else None,
                "train_line_ride_id": "-".join(s_id_split[:-1]),
                "train_line_station_num": int(s_id_split[-1]),
            }
        )
    return rows


def get_plan_db(date_folders, alternative_station_names):
    rows = []

    for date_folder_path in tqdm(date_folders, desc="Processing plan files"):
        for xml_path in sorted(date_folder_path.iterdir()):
            if "plan" in xml_path.name:
                rows.extend(get_plan_xml_rows(xml_path, alternative_station_names))

    out_df = pd.DataFrame(rows)
    out_df["arrival_planned_time"] = pd.to_datetime(
        out_df["arrival_planned_time"], format="%y%m%d%H%M", errors="coerce"
    )
    out_df["departure_planned_time"] = pd.to_datetime(
        out_df["departure_planned_time"], format="%y%m%d%H%M", errors="coerce"
    )
    out_df = out_df.drop_duplicates()
    return out_df


def get_fchg_xml_rows(xml_path, id_to_data):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    for s in root.findall("s"):
        s_id = s.get("id")
        ar_ct = s.find("ar").get("ct") if s.find("ar") is not None else None  # arrival change
        dp_ct = s.find("dp").get("ct") if s.find("dp") is not None else None  # departure change
        ar_clt = s.find("ar").get("clt") if s.find("ar") is not None else None  # arrival cancellation time
        dp_clt = s.find("dp").get("clt") if s.find("dp") is not None else None  # departure cancellation time

        if ar_clt is None and dp_clt is None:
            is_canceled = False
        else:
            is_canceled = True

        if ar_ct is None and dp_ct is None and not is_canceled:
            continue

        # overwrite older data with new data
        id_to_data[s_id] = {
            "id": s_id,
            "arrival_change_time": ar_ct,
            "departure_change_time": dp_ct,
            "is_canceled": is_canceled,
        }


def get_fchg_db(date_folders):
    id_to_data = {}

    for date_folder_path in tqdm(date_folders, desc="Processing fchg files"):
        for xml_path in sorted(date_folder_path.iterdir()):
            if "fchg" in xml_path.name:
                get_fchg_xml_rows(xml_path, id_to_data)

    out_df = pd.DataFrame(id_to_data.values())
    out_df["arrival_change_time"] = pd.to_datetime(
        out_df["arrival_change_time"], format="%y%m%d%H%M", errors="coerce"
    )
    out_df["departure_change_time"] = pd.to_datetime(
        out_df["departure_change_time"], format="%y%m%d%H%M", errors="coerce"
    )
    out_df = out_df.drop_duplicates()
    return out_df


def main(month_year):
    data_dir = Path("data")
    alternative_station_name_json = Path("alternative_station_name_to_station_name.json")

    with alternative_station_name_json.open("r") as f:
        alternative_station_names = json.load(f)

    current_month = datetime.strptime(month_year, "%Y-%m")
    prev_month_last_day = (current_month - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
    next_month_first_day = (current_month + pd.DateOffset(months=1)).strftime("%Y-%m-%d")

    date_folders = [data_dir / prev_month_last_day]
    date_folders.extend(
        [folder for folder in sorted(data_dir.iterdir()) if folder.name.startswith(month_year)]
    )
    date_folders.append(data_dir / next_month_first_day)
    date_folders = [f for f in date_folders if f.is_dir()]

    plan_df = get_plan_db(date_folders, alternative_station_names)
    fchg_df = get_fchg_db(date_folders)
    df = pd.merge(plan_df, fchg_df, on="id", how="left")

    # The default for all lines is no cancellation and the planned time is the change time.
    df["is_canceled"] = df["is_canceled"].astype("boolean").fillna(False)
    df["departure_change_time"] = df["departure_change_time"].fillna(df["departure_planned_time"])
    df["arrival_change_time"] = df["arrival_change_time"].fillna(df["arrival_planned_time"])

    # delay_in_min is the departure delay if available or else the arrival delay.
    departure_time_delta_in_min = (
        df["departure_change_time"] - df["departure_planned_time"]
    ).dt.total_seconds() / 60
    arrival_time_delta_in_min = (
        df["arrival_change_time"] - df["arrival_planned_time"]
    ).dt.total_seconds() / 60
    df["delay_in_min"] = departure_time_delta_in_min.fillna(arrival_time_delta_in_min)

    # time is the departure_change_time if available or else arrival_change_time.
    df["time"] = df["departure_change_time"].fillna(df["arrival_change_time"])

    start_date = pd.to_datetime(f"{month_year}-01")
    end_date = start_date + pd.offsets.MonthBegin(1)
    original_len = len(df)
    df = df[(df["time"] >= start_date) & (df["time"] < end_date)]
    filtered_count = original_len - len(df)
    if filtered_count > 0:
        print(f"Filtered out {filtered_count} rows with timestamps outside {month_year}")

    df = df.drop("id", axis=1)

    df = df[
        [
            "station",
            "train_name",
            "final_destination_station",
            "delay_in_min",
            "time",
            "is_canceled",
            "train_type",
            "train_line_ride_id",
            "train_line_station_num",
            "arrival_planned_time",
            "arrival_change_time",
            "departure_planned_time",
            "departure_change_time",
        ]
    ].astype(
        {
            "station": "string",
            "train_name": "string",
            "final_destination_station": "string",
            "delay_in_min": "int32",
            "is_canceled": "boolean",
            "train_type": "string",
            "train_line_ride_id": "string",
            "train_line_station_num": "int32",
        }
    )

    output_file = Path("monthly_data_releases") / f"data-{month_year}.parquet"
    df.to_parquet(
        output_file,
        index=False,
        compression="brotli",
    )
    print(f"Saved {output_file}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        month_year = sys.argv[1]
        try:
            datetime.strptime(month_year, "%Y-%m")
        except ValueError:
            print("Error: Invalid month format. Please use YYYY-MM")
            sys.exit(1)
    else:
        current_date = datetime.now()
        last_month = current_date.replace(day=1) - pd.DateOffset(days=1)
        month_year = last_month.strftime("%Y-%m")
        print(f"No month year provided, using last month: {month_year}")

    main(month_year)
