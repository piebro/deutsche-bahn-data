import json
import sys
import time
from calendar import monthrange
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree

PLAN_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("eva", pa.string(), nullable=False),
        pa.field("station_name", pa.string()),
        pa.field("xml_station_name", pa.string()),
        pa.field("snapshot_timestamp", pa.timestamp("ns"), nullable=False),
        pa.field("train_type", pa.string()),
        pa.field("train_number", pa.string()),
        pa.field("train_label_type", pa.string()),
        pa.field("train_owner", pa.string()),
        pa.field("train_filter_flags", pa.string()),
        pa.field("arrival_planned_time", pa.timestamp("ns")),
        pa.field("arrival_planned_path", pa.list_(pa.string())),
        pa.field("arrival_planned_platform", pa.string()),
        pa.field("arrival_planned_status", pa.string()),
        pa.field("arrival_line_number", pa.string()),
        pa.field("arrival_planned_distant_endpoint", pa.string()),
        pa.field("arrival_transition", pa.string()),
        pa.field("arrival_wings", pa.string()),
        pa.field("departure_planned_time", pa.timestamp("ns")),
        pa.field("departure_planned_path", pa.list_(pa.string())),
        pa.field("departure_planned_platform", pa.string()),
        pa.field("departure_planned_status", pa.string()),
        pa.field("departure_line_number", pa.string()),
        pa.field("departure_planned_distant_endpoint", pa.string()),
        pa.field("departure_transition", pa.string()),
        pa.field("departure_wings", pa.string()),
    ]
)

FCHG_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("eva", pa.string(), nullable=False),
        pa.field("station_name", pa.string()),
        pa.field("xml_station_name", pa.string()),
        pa.field("snapshot_timestamp", pa.timestamp("ns"), nullable=False),
        pa.field("train_type", pa.string()),
        pa.field("train_number", pa.string()),
        pa.field("train_label_type", pa.string()),
        pa.field("train_owner", pa.string()),
        pa.field("train_filter_flags", pa.string()),
        pa.field("arrival_planned_time", pa.timestamp("ns")),
        pa.field("arrival_change_time", pa.timestamp("ns")),
        pa.field("arrival_cancellation_time", pa.timestamp("ns")),
        pa.field("arrival_planned_path", pa.list_(pa.string())),
        pa.field("arrival_change_path", pa.list_(pa.string())),
        pa.field("arrival_planned_platform", pa.string()),
        pa.field("arrival_change_platform", pa.string()),
        pa.field("arrival_planned_status", pa.string()),
        pa.field("arrival_change_status", pa.string()),
        pa.field("arrival_line_number", pa.string()),
        pa.field("arrival_planned_distant_endpoint", pa.string()),
        pa.field("arrival_transition", pa.string()),
        pa.field("arrival_wings", pa.string()),
        pa.field("departure_planned_time", pa.timestamp("ns")),
        pa.field("departure_change_time", pa.timestamp("ns")),
        pa.field("departure_cancellation_time", pa.timestamp("ns")),
        pa.field("departure_planned_path", pa.list_(pa.string())),
        pa.field("departure_change_path", pa.list_(pa.string())),
        pa.field("departure_planned_platform", pa.string()),
        pa.field("departure_change_platform", pa.string()),
        pa.field("departure_planned_status", pa.string()),
        pa.field("departure_change_status", pa.string()),
        pa.field("departure_line_number", pa.string()),
        pa.field("departure_planned_distant_endpoint", pa.string()),
        pa.field("departure_transition", pa.string()),
        pa.field("departure_wings", pa.string()),
        pa.field("replaced_train_type", pa.string()),
        pa.field("replaced_train_number", pa.string()),
        pa.field("replaced_train_label_type", pa.string()),
        pa.field("replaced_train_owner", pa.string()),
        pa.field("replaced_train_filter_flags", pa.string()),
    ]
)


def to_datetime(value: str | None) -> pd.Timestamp | None:
    if value is None:
        return None
    return pd.to_datetime(value, format="%y%m%d%H%M", errors="coerce")


def to_path(value: str | None) -> list[str] | None:
    if value is None:
        return None
    if not value:
        return []
    return value.split("|")


def train_fields(train_label, prefix: str = "") -> dict:
    # tl.n is the Zugnummer for a specific train run; tl.c is its category (ICE, RE, etc.).
    return {
        f"{prefix}train_type": train_label.get("c") if train_label is not None else None,
        f"{prefix}train_number": train_label.get("n") if train_label is not None else None,
        f"{prefix}train_label_type": train_label.get("t") if train_label is not None else None,
        f"{prefix}train_owner": train_label.get("o") if train_label is not None else None,
        f"{prefix}train_filter_flags": train_label.get("f") if train_label is not None else None,
    }


def planned_event_fields(event, prefix: str) -> dict:
    # Event attribute l is the route's Liniennummer and is distinct from the Zugnummer.
    return {
        f"{prefix}_planned_time": to_datetime(event.get("pt")) if event is not None else None,
        f"{prefix}_planned_path": to_path(event.get("ppth")) if event is not None else None,
        f"{prefix}_planned_platform": event.get("pp") if event is not None else None,
        f"{prefix}_planned_status": event.get("ps") if event is not None else None,
        f"{prefix}_line_number": event.get("l") if event is not None else None,
        f"{prefix}_planned_distant_endpoint": event.get("pde") if event is not None else None,
        f"{prefix}_transition": event.get("tra") if event is not None else None,
        f"{prefix}_wings": event.get("wings") if event is not None else None,
    }


def changed_event_fields(event, prefix: str) -> dict:
    return {
        f"{prefix}_change_time": to_datetime(event.get("ct")) if event is not None else None,
        f"{prefix}_cancellation_time": to_datetime(event.get("clt")) if event is not None else None,
        f"{prefix}_change_path": to_path(event.get("cpth")) if event is not None else None,
        f"{prefix}_change_platform": event.get("cp") if event is not None else None,
        f"{prefix}_change_status": event.get("cs") if event is not None else None,
    }


def get_plan_xml_rows(xml_string: str, eva: str, station_name: str | None, snapshot_timestamp) -> list[dict]:
    root = etree.fromstring(xml_string.encode())
    xml_station_name = root.get("station")

    rows = []
    for stop in root.findall("s"):
        row = {
            "id": stop.get("id"),
            "eva": eva,
            "station_name": station_name,
            "xml_station_name": xml_station_name,
            "snapshot_timestamp": snapshot_timestamp,
        }
        row.update(train_fields(stop.find("tl")))
        row.update(planned_event_fields(stop.find("ar"), "arrival"))
        row.update(planned_event_fields(stop.find("dp"), "departure"))
        rows.append(row)
    return rows


def get_fchg_xml_rows(xml_string: str, eva: str, station_name: str | None, snapshot_timestamp) -> list[dict]:
    root = etree.fromstring(xml_string.encode())
    xml_station_name = root.get("station")

    rows = []
    for stop in root.findall("s"):
        train_label = stop.find("tl")
        arrival = stop.find("ar")
        departure = stop.find("dp")
        reference_train_label = stop.find("ref/tl")

        # Elements without attributes contain only nested messages, which are outside
        # the parsed-data contract.
        if (
            not (arrival is not None and arrival.attrib)
            and not (departure is not None and departure.attrib)
            and reference_train_label is None
            and (train_label is None or train_label.get("t") != "e")
        ):
            continue

        row = {
            "id": stop.get("id"),
            "eva": eva,
            "station_name": station_name,
            "xml_station_name": xml_station_name,
            "snapshot_timestamp": snapshot_timestamp,
        }
        # For replacement trains, tl describes the train that ran while ref/tl
        # identifies the scheduled train it replaced.
        row.update(train_fields(train_label))
        # Additional and replacement stops carry their planned data in fchg itself.
        row.update(planned_event_fields(arrival, "arrival"))
        row.update(changed_event_fields(arrival, "arrival"))
        row.update(planned_event_fields(departure, "departure"))
        row.update(changed_event_fields(departure, "departure"))
        row.update(train_fields(reference_train_label, "replaced_"))
        rows.append(row)
    return rows


def _get_eva(url: str, api_name: str) -> str:
    prefix = f"https://apis.deutschebahn.com/db-api-marketplace/apis/{api_name}/"
    return url.removeprefix(prefix).split("/")[0]


def get_plan_rows(xml_df: pd.DataFrame, eva_to_station: dict[str, str]) -> list[dict]:
    rows = []
    for row in xml_df[xml_df["api_name"] == "timetables/v1/plan"].itertuples():
        if row.response_data:
            eva = _get_eva(row.url, row.api_name)
            rows.extend(get_plan_xml_rows(row.response_data, eva, eva_to_station.get(eva), row.timestamp))
    return rows


def get_fchg_rows(xml_df: pd.DataFrame, eva_to_station: dict[str, str]) -> list[dict]:
    rows = []
    for row in xml_df[xml_df["api_name"] == "timetables/v1/fchg"].itertuples():
        if row.response_data:
            eva = _get_eva(row.url, row.api_name)
            rows.extend(get_fchg_xml_rows(row.response_data, eva, eva_to_station.get(eva), row.timestamp))
    return rows


def get_parquet_files(year: int, month: int) -> list[Path]:
    """Return target-month raw files plus the adjacent boundary days."""
    prev_month = 12 if month == 1 else month - 1
    prev_year = year - 1 if month == 1 else year
    last_day_prev = monthrange(prev_year, prev_month)[1]
    next_month = 1 if month == 12 else month + 1
    next_year = year + 1 if month == 12 else year

    parquet_files = []
    parquet_files.extend(Path(f"raw_data/year={prev_year}/month={prev_month}/day={last_day_prev}").rglob("*.parquet"))
    parquet_files.extend(Path(f"raw_data/year={year}/month={month}").rglob("*.parquet"))
    parquet_files.extend(Path(f"raw_data/year={next_year}/month={next_month}/day=1").rglob("*.parquet"))

    def sort_key(path: Path):
        parts = {part.split("=")[0]: int(part.split("=")[1]) for part in path.parts if "=" in part}
        return (parts.get("year", 0), parts.get("month", 0), parts.get("day", 0), path.name)

    return sorted(parquet_files, key=sort_key)


def _write_empty_file(path: Path, schema: pa.Schema) -> None:
    pq.write_table(pa.Table.from_pylist([], schema=schema), path, compression="zstd", compression_level=3)


def main(
    year: int,
    month: int,
    parquet_files: list[Path],
    eva_to_station: dict[str, str],
    plan_dir: Path,
    change_dir: Path,
) -> tuple[Path, Path]:
    start_time = time.time()
    plan_dir.mkdir(parents=True, exist_ok=True)
    change_dir.mkdir(parents=True, exist_ok=True)

    plan_output = plan_dir / f"data-{year}-{month:02d}.parquet"
    fchg_output = change_dir / f"data-{year}-{month:02d}.parquet"
    plan_temp = plan_output.with_suffix(".tmp.parquet")
    fchg_temp = fchg_output.with_suffix(".tmp.parquet")
    plan_temp.unlink(missing_ok=True)
    fchg_temp.unlink(missing_ok=True)

    plan_writer = None
    fchg_writer = None
    total_xml_count = 0
    total_plan_count = 0
    total_fchg_count = 0

    try:
        for parquet_file in parquet_files:
            xml_df = pd.read_parquet(parquet_file)
            xml_df = xml_df[xml_df["status_code"] == "200"]
            total_xml_count += len(xml_df)

            plan_rows = get_plan_rows(xml_df, eva_to_station)
            if plan_rows:
                if plan_writer is None:
                    plan_writer = pq.ParquetWriter(plan_temp, PLAN_SCHEMA, compression="zstd", compression_level=3)
                plan_writer.write_table(pa.Table.from_pylist(plan_rows, schema=PLAN_SCHEMA))
                total_plan_count += len(plan_rows)
            del plan_rows

            fchg_rows = get_fchg_rows(xml_df, eva_to_station)
            if fchg_rows:
                if fchg_writer is None:
                    fchg_writer = pq.ParquetWriter(fchg_temp, FCHG_SCHEMA, compression="zstd", compression_level=3)
                fchg_writer.write_table(pa.Table.from_pylist(fchg_rows, schema=FCHG_SCHEMA))
                total_fchg_count += len(fchg_rows)
            del fchg_rows, xml_df
    finally:
        if plan_writer is not None:
            plan_writer.close()
        if fchg_writer is not None:
            fchg_writer.close()

    if not plan_temp.exists():
        _write_empty_file(plan_temp, PLAN_SCHEMA)
    if not fchg_temp.exists():
        _write_empty_file(fchg_temp, FCHG_SCHEMA)

    plan_temp.replace(plan_output)
    fchg_temp.replace(fchg_output)

    print(f"There are {total_xml_count:_} successful API responses")
    print(f"Containing {total_plan_count:_} plan stop snapshots")
    print(f"Containing {total_fchg_count:_} change stop snapshots")
    print(f"Saved parsed plan data to {plan_output}")
    print(f"Saved parsed change data to {fchg_output}")
    print(f"Total parsing time: {time.time() - start_time:.2f} seconds")
    return plan_output, fchg_output


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: uv run python scripts/parse_monthly_xml.py <year> <month>")
        sys.exit(1)

    input_year = int(sys.argv[1])
    input_month = int(sys.argv[2])
    station_names = json.loads(Path("config/eva_to_station_name.json").read_text())
    input_files = get_parquet_files(input_year, input_month)
    main(
        input_year,
        input_month,
        input_files,
        station_names,
        Path("monthly_processed_data_plan"),
        Path("monthly_processed_data_change"),
    )
