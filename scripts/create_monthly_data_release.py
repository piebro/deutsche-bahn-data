import json
import shutil
import sys
import time
from calendar import monthrange
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from lxml import etree


def to_datetime(datetime_str: str):
    if datetime_str is None:
        return None
    return pd.to_datetime(datetime_str, format="%y%m%d%H%M", errors="coerce")


def get_plan_xml_rows(xml_string: str, eva: str, station_name: dict[str, str], xml_timestamp) -> list[dict]:
    root = etree.fromstring(xml_string.encode())
    xml_station_name = root.get("station")

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

        dp_ppth = s.find("dp").get("ppth") if s.find("dp") is not None else None  # departure planned path
        if dp_ppth is None:
            final_destination_station = station_name
        else:
            final_destination_station = dp_ppth.split("|")[-1]

        ar_pt = s.find("ar").get("pt") if s.find("ar") is not None else None
        dp_pt = s.find("dp").get("pt") if s.find("dp") is not None else None

        rows.append(
            {
                "id": s_id,
                "station_name": station_name,
                "xml_station_name": xml_station_name,
                "eva": eva,
                "train_name": train_name,
                "final_destination_station": final_destination_station,
                "train_type": train_type,
                "arrival_planned_time": to_datetime(ar_pt),
                "departure_planned_time": to_datetime(dp_pt),
                "xml_timestamp": xml_timestamp,
            }
        )
    return rows


def get_plan_db(xml_df, eva_to_station):
    raw_plan_df = xml_df[(xml_df["api_name"] == "timetables/v1/plan")]
    rows = []
    for row in raw_plan_df.itertuples():
        if row.response_data:
            prefix = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/"
            eva = row.url.removeprefix(prefix).split("/")[0]
            rows.extend(get_plan_xml_rows(row.response_data, eva, eva_to_station.get(eva, None), row.timestamp))

    plan_df = pd.DataFrame(rows)
    return plan_df


def get_fchg_xml_rows(xml_string: str, xml_timestamp) -> list[dict]:
    root = etree.fromstring(xml_string.encode())

    rows = []
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

        rows.append(
            {
                "id": s_id,
                "arrival_change_time": to_datetime(ar_ct),
                "departure_change_time": to_datetime(dp_ct),
                "is_canceled": is_canceled,
                "xml_timestamp": xml_timestamp,
            }
        )
    return rows


def get_fchg_db(xml_df, eva_to_station):
    raw_fchg_df = xml_df[(xml_df["api_name"] == "timetables/v1/fchg")]

    rows = []
    for row in raw_fchg_df.itertuples():
        if row.response_data:
            rows.extend(get_fchg_xml_rows(row.response_data, row.timestamp))

    fchg_df = pd.DataFrame(rows)
    return fchg_df


def get_parquet_files(year: int, month: int):
    """Get all parquet files of the month and last day of prev month and first day of next month."""
    parquet_files = []

    # Determine previous month/year and its last day
    prev_month = 12 if month == 1 else month - 1
    prev_year = year - 1 if month == 1 else year
    last_day_prev = monthrange(prev_year, prev_month)[1]

    # Determine next month/year
    next_month = 1 if month == 12 else month + 1
    next_year = year + 1 if month == 12 else year

    # Get last day of previous month
    prev_day_path = Path(f"raw_data/year={prev_year}/month={prev_month}/day={last_day_prev}")
    parquet_files.extend(prev_day_path.rglob("*.parquet"))

    # Get all days from target month
    target_month_path = Path(f"raw_data/year={year}/month={month}")
    parquet_files.extend(target_month_path.rglob("*.parquet"))

    # Get first day of next month
    next_day_path = Path(f"raw_data/year={next_year}/month={next_month}/day=1")
    parquet_files.extend(next_day_path.rglob("*.parquet"))

    # Sort chronologically by extracting year, month, day from path, then by full path string (for the hours)
    def sort_key(path):
        parts = {part.split("=")[0]: int(part.split("=")[1]) for part in path.parts if "=" in part}
        return (parts.get("year", 0), parts.get("month", 0), parts.get("day", 0), path.name)

    return sorted(parquet_files, key=sort_key)


def process_files_to_temp(parquet_files: list[Path], eva_to_station: dict[str, str], temp_dir: Path):
    """Process parquet files one by one and write plan/fchg data to temp directories."""
    plan_dir = temp_dir / "plan"
    fchg_dir = temp_dir / "fchg"
    plan_dir.mkdir(parents=True, exist_ok=True)
    fchg_dir.mkdir(parents=True, exist_ok=True)

    total_xml_count = 0
    total_plan_count = 0
    total_fchg_count = 0

    for i, parquet_file in enumerate(parquet_files):
        # Read one file at a time
        xml_df = pd.read_parquet(parquet_file)
        xml_df = xml_df[xml_df["status_code"] == "200"]
        total_xml_count += len(xml_df)

        # Process plan data
        plan_df = get_plan_db(xml_df, eva_to_station)
        if len(plan_df) > 0:
            plan_output = plan_dir / f"batch_{i:05d}.parquet"
            plan_df.to_parquet(plan_output, index=False)
            total_plan_count += len(plan_df)

        # Process fchg data
        fchg_df = get_fchg_db(xml_df, eva_to_station)
        if len(fchg_df) > 0:
            fchg_output = fchg_dir / f"batch_{i:05d}.parquet"
            fchg_df.to_parquet(fchg_output, index=False)
            total_fchg_count += len(fchg_df)

        # Clear memory
        del xml_df, plan_df, fchg_df

    return total_xml_count, total_plan_count, total_fchg_count


def main(year: int, month: int, parquet_files, eva_to_station: dict, output_dir: Path):
    start_time = time.time()

    # Setup paths
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / f"data-{year}-{month:02d}.parquet"
    temp_dir = output_dir / "temp_monthly_processing"

    # Process files one by one and write to temp directories
    total_xml_count, total_plan_count, total_fchg_count = process_files_to_temp(parquet_files, eva_to_station, temp_dir)

    print(f"There are {total_xml_count:_} valid .xml strings")
    print(f"Containing {total_plan_count:_} planed schedules")
    print(f"Containing {total_fchg_count:_} change schedules")

    # Use DuckDB to merge, transform, filter, and save directly to parquet
    plan_pattern = str(temp_dir / "plan" / "*.parquet")
    fchg_pattern = str(temp_dir / "fchg" / "*.parquet")

    # Calculate date range for filtering
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)

    duckdb.sql(f"""
        COPY (
            WITH plan_deduped AS (
                SELECT DISTINCT ON (id)
                    id,
                    station_name,
                    xml_station_name,
                    eva,
                    train_name,
                    final_destination_station,
                    train_type,
                    arrival_planned_time,
                    departure_planned_time
                FROM '{plan_pattern}'
                ORDER BY id, xml_timestamp DESC
            ),
            fchg_deduped AS (
                SELECT DISTINCT ON (id)
                    id,
                    arrival_change_time,
                    departure_change_time,
                    is_canceled
                FROM '{fchg_pattern}'
                ORDER BY id, xml_timestamp DESC
            ),
            merged AS (
                SELECT
                    p.id,
                    p.station_name,
                    p.xml_station_name,
                    p.eva,
                    p.train_name,
                    p.final_destination_station,
                    p.train_type,
                    p.arrival_planned_time,
                    p.departure_planned_time,
                    COALESCE(f.arrival_change_time, p.arrival_planned_time) AS arrival_change_time,
                    COALESCE(f.departure_change_time, p.departure_planned_time) AS departure_change_time,
                    COALESCE(f.is_canceled, false) AS is_canceled
                FROM plan_deduped p
                LEFT JOIN fchg_deduped f ON p.id = f.id
            ),
            transformed AS (
                SELECT
                    station_name,
                    xml_station_name,
                    eva,
                    train_name,
                    final_destination_station,
                    CAST(COALESCE(
                        date_diff('minute', departure_planned_time, departure_change_time),
                        date_diff('minute', arrival_planned_time, arrival_change_time)
                    ) AS INTEGER) AS delay_in_min,
                    COALESCE(departure_change_time, arrival_change_time) AS time,
                    is_canceled,
                    train_type,
                    split_part(id, '-', 1) AS train_line_ride_id,
                    CAST(split_part(id, '-', -1) AS INTEGER) AS train_line_station_num,
                    arrival_planned_time,
                    arrival_change_time,
                    departure_planned_time,
                    departure_change_time,
                    id
                FROM merged
                ORDER BY time
            )
            SELECT * FROM transformed
            WHERE time >= TIMESTAMP '{start_date.strftime("%Y-%m-%d %H:%M:%S")}'
                AND time < TIMESTAMP '{end_date.strftime("%Y-%m-%d %H:%M:%S")}'
        ) TO '{output_file}' (FORMAT PARQUET)
    """)

    print(f"Saved records to {output_file}")

    # Clean up temp directory
    shutil.rmtree(temp_dir)
    print(f"Total processing time: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: uv run scripts/create_monthly_data_release.py <year> <month>")
        sys.exit(1)

    year = int(sys.argv[1])
    month = int(sys.argv[2])

    eva_to_station = json.load(open("config/eva_to_station_name.json"))
    parquet_files = get_parquet_files(year, month)
    main(year, month, parquet_files, eva_to_station, output_dir=Path("monthly_processed_data"))
