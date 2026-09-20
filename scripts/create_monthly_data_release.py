import json
import shutil
import sys
import time
from calendar import monthrange
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
from lxml import etree


def to_datetime(datetime_str: str | None) -> pd.Timestamp | None:
    if datetime_str is None:
        return None
    return pd.to_datetime(datetime_str, format="%y%m%d%H%M", errors="coerce")


# Explicit pyarrow schemas for the plan/fchg batch parquet files. Every batch file of
# the same kind is written with the same schema, so an all-None column (which pandas
# would otherwise write with an unrelated inferred type, e.g. INTEGER) is always
# stored as the intended type. This keeps DuckDB's multi-file glob reads and the
# plan/fchg COALESCE merge type-consistent across all batches of a month.
PLAN_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=True),
        pa.field("station_name", pa.string(), nullable=True),
        pa.field("xml_station_name", pa.string(), nullable=True),
        pa.field("eva", pa.string(), nullable=True),
        pa.field("train_number", pa.string(), nullable=True),
        pa.field("line_number", pa.string(), nullable=True),
        pa.field("final_destination_station", pa.string(), nullable=True),
        pa.field("train_type", pa.string(), nullable=True),
        pa.field("arrival_planned_time", pa.timestamp("ns"), nullable=True),
        pa.field("departure_planned_time", pa.timestamp("ns"), nullable=True),
        pa.field("xml_timestamp", pa.timestamp("ns"), nullable=True),
    ]
)

FCHG_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=True),
        pa.field("station_name", pa.string(), nullable=True),
        pa.field("xml_station_name", pa.string(), nullable=True),
        pa.field("eva", pa.string(), nullable=True),
        pa.field("train_number", pa.string(), nullable=True),
        pa.field("line_number", pa.string(), nullable=True),
        pa.field("final_destination_station", pa.string(), nullable=True),
        pa.field("train_type", pa.string(), nullable=True),
        pa.field("replaced_train_number", pa.string(), nullable=True),
        pa.field("arrival_planned_time", pa.timestamp("ns"), nullable=True),
        pa.field("departure_planned_time", pa.timestamp("ns"), nullable=True),
        pa.field("arrival_change_time", pa.timestamp("ns"), nullable=True),
        pa.field("departure_change_time", pa.timestamp("ns"), nullable=True),
        pa.field("arrival_is_canceled", pa.bool_(), nullable=True),
        pa.field("departure_is_canceled", pa.bool_(), nullable=True),
        pa.field("is_additional_stop", pa.bool_(), nullable=True),
        pa.field("is_replacement_train", pa.bool_(), nullable=True),
        pa.field("xml_timestamp", pa.timestamp("ns"), nullable=True),
    ]
)


def get_plan_xml_rows(xml_string: str, eva: str, station_name: dict[str, str], xml_timestamp) -> list[dict]:
    root = etree.fromstring(xml_string.encode())
    xml_station_name = root.get("station")

    rows = []
    for s in root.findall("s"):
        s_id = s.get("id")
        tl = s.find("tl")
        ar = s.find("ar")
        dp = s.find("dp")

        train_type = tl.get("c") if tl is not None else None
        # train_number is the Zugnummer (tl.n), identifying a specific train run
        train_number = tl.get("n") if tl is not None else None
        # line_number is the Liniennummer (ar.l / dp.l), identifying the route; it
        # groups multiple runs and is absent for long-distance trains (ICE/IC/EC).
        ar_line = ar.get("l") if ar is not None else None
        dp_line = dp.get("l") if dp is not None else None
        line_number = ar_line if ar_line is not None else dp_line

        # final_destination_station is derived from dp.ppth (departure planned path); it
        # stays None when ppth is unavailable, rather than falling back to the current
        # station, so that change-only rows don't falsely look like they terminate here.
        dp_ppth = dp.get("ppth") if dp is not None else None
        final_destination_station = dp_ppth.split("|")[-1] if dp_ppth is not None else None

        ar_pt = ar.get("pt") if ar is not None else None
        dp_pt = dp.get("pt") if dp is not None else None

        rows.append(
            {
                "id": s_id,
                "station_name": station_name,
                "xml_station_name": xml_station_name,
                "eva": eva,
                "train_number": train_number,
                "line_number": line_number,
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

    return pd.DataFrame(rows)


def get_fchg_xml_rows(xml_string: str, eva: str, station_name: dict[str, str], xml_timestamp) -> list[dict]:
    root = etree.fromstring(xml_string.encode())
    xml_station_name = root.get("station")

    rows = []
    for s in root.findall("s"):
        s_id = s.get("id")
        tl = s.find("tl")
        ar = s.find("ar")
        dp = s.find("dp")

        train_type = tl.get("c") if tl is not None else None
        # train_number is the Zugnummer (tl.n). For replacement trains this is the
        # number of the train that actually ran (t="e"), not the planned one.
        train_number = tl.get("n") if tl is not None else None
        tl_t = tl.get("t") if tl is not None else None
        ar_line = ar.get("l") if ar is not None else None
        dp_line = dp.get("l") if dp is not None else None
        line_number = ar_line if ar_line is not None else dp_line

        # ps="a" marks a stop that is not on the scheduled path (extra/diversion stop).
        ar_ps = ar.get("ps") if ar is not None else None
        dp_ps = dp.get("ps") if dp is not None else None
        is_additional_stop = ar_ps == "a" or dp_ps == "a"

        # Replacement trains carry a <ref> element pointing at the train they replace.
        ref = s.find("ref")
        is_replacement_train = tl_t == "e" or ref is not None
        replaced_train_number = None
        if ref is not None:
            ref_tl = ref.find("tl")
            if ref_tl is not None:
                replaced_train_number = ref_tl.get("n")

        # final_destination_station is derived from dp.ppth (departure planned path); it
        # stays None when ppth is unavailable (e.g. change-only rows where the plan data
        # is missing), rather than falling back to the current station.
        dp_ppth = dp.get("ppth") if dp is not None else None
        final_destination_station = dp_ppth.split("|")[-1] if dp_ppth is not None else None

        ar_ct = ar.get("ct") if ar is not None else None  # arrival change
        dp_ct = dp.get("ct") if dp is not None else None  # departure change
        ar_clt = ar.get("clt") if ar is not None else None  # arrival cancellation time
        dp_clt = dp.get("clt") if dp is not None else None  # departure cancellation time
        # For extra and replacement stops the planned time (pt) is present in fchg itself.
        ar_pt = ar.get("pt") if ar is not None else None  # arrival planned
        dp_pt = dp.get("pt") if dp is not None else None  # departure planned

        arrival_is_canceled = ar_clt is not None
        departure_is_canceled = dp_clt is not None

        # Only keep rows that carry actual stop information: a changed/canceled time,
        # an extra stop, or a replacement train. Discard pure info/message rows.
        if (
            ar_ct is None
            and dp_ct is None
            and not arrival_is_canceled
            and not departure_is_canceled
            and not is_additional_stop
            and not is_replacement_train
        ):
            continue

        rows.append(
            {
                "id": s_id,
                "station_name": station_name,
                "xml_station_name": xml_station_name,
                "eva": eva,
                "train_number": train_number,
                "line_number": line_number,
                "final_destination_station": final_destination_station,
                "train_type": train_type,
                "arrival_planned_time": to_datetime(ar_pt),
                "departure_planned_time": to_datetime(dp_pt),
                "arrival_change_time": to_datetime(ar_ct),
                "departure_change_time": to_datetime(dp_ct),
                "arrival_is_canceled": arrival_is_canceled,
                "departure_is_canceled": departure_is_canceled,
                "is_additional_stop": is_additional_stop,
                "is_replacement_train": is_replacement_train,
                "replaced_train_number": replaced_train_number,
                "xml_timestamp": xml_timestamp,
            }
        )
    return rows


def get_fchg_db(xml_df, eva_to_station):
    raw_fchg_df = xml_df[(xml_df["api_name"] == "timetables/v1/fchg")]

    rows = []
    for row in raw_fchg_df.itertuples():
        if row.response_data:
            prefix = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/"
            eva = row.url.removeprefix(prefix).split("/")[0]
            rows.extend(get_fchg_xml_rows(row.response_data, eva, eva_to_station.get(eva, None), row.timestamp))

    return pd.DataFrame(rows)


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
            plan_df.to_parquet(plan_output, index=False, schema=PLAN_SCHEMA)
            total_plan_count += len(plan_df)

        # Process fchg data
        fchg_df = get_fchg_db(xml_df, eva_to_station)
        if len(fchg_df) > 0:
            fchg_output = fchg_dir / f"batch_{i:05d}.parquet"
            fchg_df.to_parquet(fchg_output, index=False, schema=FCHG_SCHEMA)
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
                    train_number,
                    line_number,
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
                    station_name,
                    xml_station_name,
                    eva,
                    train_number,
                    line_number,
                    final_destination_station,
                    train_type,
                    arrival_planned_time,
                    departure_planned_time,
                    arrival_change_time,
                    departure_change_time,
                    arrival_is_canceled,
                    departure_is_canceled,
                    is_additional_stop,
                    is_replacement_train,
                    replaced_train_number
                FROM '{fchg_pattern}'
                ORDER BY id, xml_timestamp DESC
            ),
            merged AS (
                SELECT
                    COALESCE(p.id, f.id) AS id,
                    COALESCE(p.station_name, f.station_name) AS station_name,
                    COALESCE(p.xml_station_name, f.xml_station_name) AS xml_station_name,
                    COALESCE(p.eva, f.eva) AS eva,
                    COALESCE(p.train_number, f.train_number) AS train_number,
                    COALESCE(p.line_number, f.line_number) AS line_number,
                    COALESCE(p.final_destination_station, f.final_destination_station) AS final_destination_station,
                    COALESCE(p.train_type, f.train_type) AS train_type,
                    COALESCE(p.arrival_planned_time, f.arrival_planned_time) AS arrival_planned_time,
                    COALESCE(p.departure_planned_time, f.departure_planned_time) AS departure_planned_time,
                    COALESCE(f.arrival_change_time, p.arrival_planned_time, f.arrival_planned_time) AS arrival_change_time,
                    COALESCE(f.departure_change_time, p.departure_planned_time, f.departure_planned_time) AS departure_change_time,
                    COALESCE(f.arrival_is_canceled, false) AS arrival_is_canceled,
                    COALESCE(f.departure_is_canceled, false) AS departure_is_canceled,
                    COALESCE(f.is_additional_stop, false) AS is_additional_stop,
                    COALESCE(f.is_replacement_train, false) AS is_replacement_train,
                    f.replaced_train_number AS replaced_train_number
                FROM plan_deduped p
                FULL OUTER JOIN fchg_deduped f ON p.id = f.id
            ),
            transformed AS (
                SELECT
                    station_name,
                    xml_station_name,
                    eva,
                    train_number,
                    line_number,
                    final_destination_station,
                    CAST(COALESCE(
                        date_diff('minute', departure_planned_time, departure_change_time),
                        date_diff('minute', arrival_planned_time, arrival_change_time)
                    ) AS INTEGER) AS delay_in_min,
                    COALESCE(departure_change_time, arrival_change_time) AS time,
                    arrival_is_canceled,
                    departure_is_canceled,
                    train_type,
                    is_additional_stop,
                    is_replacement_train,
                    replaced_train_number,
                    regexp_extract(id, '^(.*)-\\d{{10}}-\\d+$', 1) AS train_line_ride_id,
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
