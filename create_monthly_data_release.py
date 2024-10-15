import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
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

        ar_pp = s.find("ar").get("pp") if s.find("ar") is not None else None
        dp_pp = s.find("dp").get("pp") if s.find("dp") is not None else None
        planned_platform = dp_pp or ar_pp  # `or` to select the first non-None value

        s_id_split = s_id.split("-")

        dp_ppth = s.find("dp").get("ppth") if s.find("dp") is not None else None  # departure planed path
        if dp_ppth is None:
            final_destination_station = station
        else:
            final_destination_station = dp_ppth.split("|")[-1]

        # arrival_planned_path = s.find('ar').get('ppth') if s.find('ar') is not None else None
        # departure_planned_path = s.find('dp').get('ppth') if s.find('dp') is not None else None
        # if arrival_planned_path is None:
        #     train_path = f"{station}|{departure_planned_path}"
        # elif departure_planned_path is None:
        #     train_path = f"{arrival_planned_path}|{station}"
        # else:
        #     train_path = f"{arrival_planned_path}|{station}|{departure_planned_path}"

        rows.append(
            {
                "id": s_id,
                "station": station,
                "train_name": train_name,
                "final_destination_station": final_destination_station,
                "train_type": train_type,
                "arrival_planned_time": s.find("ar").get("pt") if s.find("ar") is not None else None,
                "departure_planned_time": s.find("dp").get("pt") if s.find("dp") is not None else None,
                "planned_platform": planned_platform,
                "train_line_ride_id": "-".join(s_id_split[:-1]),
                "train_line_station_num": int(s_id_split[-1]),
            }
        )
    return rows


def get_plan_db(data_dir, alternative_station_names, month_year):
    rows = []
    date_folders = [
        folder for folder in sorted(data_dir.iterdir())
        if folder.name.startswith(month_year)
    ]
    
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

        # arrival or departure changed platform
        ar_cp = s.find("ar").get("cp") if s.find("ar") is not None else None
        dp_cp = s.find("dp").get("cp") if s.find("dp") is not None else None
        changed_platform = ar_cp or dp_cp

        if ar_ct is None and dp_ct is None and changed_platform is None and not is_canceled:
            continue

        # overwrite older data with new data
        id_to_data[s_id] = {
            "id": s_id,
            "arrival_change_time": ar_ct,
            "departure_change_time": dp_ct,
            "is_canceled": is_canceled,
            "changed_platform": changed_platform,
        }


def get_fchg_db(data_dir, month_year):
    id_to_data = {}
    current_month = datetime.strptime(month_year, "%Y-%m")
    next_month = current_month.replace(day=1) + pd.DateOffset(months=1)
    next_month_first_day_str = next_month.strftime("%Y-%m") + "-01"

    date_folders = [
        folder for folder in sorted(data_dir.iterdir())
        if (folder.name.startswith(month_year) or folder.name.startswith(next_month_first_day_str))
    ]

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

    plan_df = get_plan_db(data_dir, alternative_station_names, month_year)
    fchg_df = get_fchg_db(data_dir, month_year)
    df = pd.merge(plan_df, fchg_df, on="id", how="left")

    df.loc[df["arrival_planned_time"] == df["arrival_change_time"], "arrival_change_time"] = None
    df.loc[df["departure_planned_time"] == df["departure_change_time"], "departure_change_time"] = None

    # Calculate time deltas
    df["arrival_time_delta_in_min"] = (
        df["arrival_change_time"] - df["arrival_planned_time"]
    ).dt.total_seconds() / 60
    df["departure_time_delta_in_min"] = (
        df["departure_change_time"] - df["departure_planned_time"]
    ).dt.total_seconds() / 60
    df["delay_in_min"] = df["arrival_time_delta_in_min"].fillna(df["departure_time_delta_in_min"])
    df["delay_in_min"] = (df["delay_in_min"].fillna(df["departure_time_delta_in_min"]).fillna(0)).astype(int)

    df["is_endstation"] = df["station"] == df["final_destination_station"]
    df.loc[df["is_canceled"].isna(), "is_canceled"] = False

    df = df.drop("id", axis=1)

    # Reorder columns as per the new order specified
    df = df[
        [
            "station",
            "train_name",
            "final_destination_station",
            "delay_in_min",
            "arrival_planned_time",
            "arrival_time_delta_in_min",
            "departure_planned_time",
            "departure_time_delta_in_min",
            "planned_platform",
            "changed_platform",
            "is_canceled",
            "train_type",
            "train_line_ride_id",
            "train_line_station_num",
            "is_endstation",
        ]
    ]

    output_file = Path("monthly_data_releases")/f"data-{month_year}.parquet"
    df.to_parquet(output_file, index=False)
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
