import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb


def main(year: int, month: int, plan_file: Path, fchg_file: Path, output_dir: Path) -> Path:
    start_time = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"data-{year}-{month:02d}.parquet"
    temp_output = output_file.with_suffix(".tmp.parquet")
    temp_output.unlink(missing_ok=True)

    start_date = datetime(year, month, 1)
    end_date = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)

    duckdb.sql(f"""
        COPY (
            WITH plan_deduped AS (
                SELECT DISTINCT ON (id)
                    id,
                    station_name,
                    xml_station_name,
                    eva,
                    train_number,
                    arrival_line_number,
                    departure_line_number,
                    departure_planned_path,
                    train_type,
                    arrival_planned_time,
                    departure_planned_time
                FROM '{plan_file}'
                ORDER BY id, snapshot_timestamp DESC
            ),
            fchg_deduped AS (
                SELECT DISTINCT ON (id)
                    id,
                    station_name,
                    xml_station_name,
                    eva,
                    train_number,
                    arrival_line_number,
                    departure_line_number,
                    departure_planned_path,
                    train_type,
                    train_label_type,
                    arrival_planned_time,
                    departure_planned_time,
                    arrival_change_time,
                    departure_change_time,
                    arrival_cancellation_time,
                    departure_cancellation_time,
                    arrival_planned_status,
                    departure_planned_status,
                    arrival_change_status,
                    departure_change_status,
                    replaced_train_type,
                    replaced_train_number,
                    replaced_train_label_type,
                    replaced_train_owner,
                    replaced_train_filter_flags
                FROM '{fchg_file}'
                WHERE arrival_change_time IS NOT NULL
                    OR departure_change_time IS NOT NULL
                    OR arrival_cancellation_time IS NOT NULL
                    OR departure_cancellation_time IS NOT NULL
                    OR arrival_planned_status = 'a'
                    OR departure_planned_status = 'a'
                    OR arrival_change_status IS NOT NULL
                    OR departure_change_status IS NOT NULL
                    OR train_label_type = 'e'
                    OR replaced_train_type IS NOT NULL
                    OR replaced_train_number IS NOT NULL
                    OR replaced_train_label_type IS NOT NULL
                    OR replaced_train_owner IS NOT NULL
                    OR replaced_train_filter_flags IS NOT NULL
                ORDER BY id, snapshot_timestamp DESC
            ),
            merged AS (
                SELECT
                    COALESCE(p.id, f.id) AS id,
                    COALESCE(p.station_name, f.station_name) AS station_name,
                    COALESCE(p.xml_station_name, f.xml_station_name) AS xml_station_name,
                    COALESCE(p.eva, f.eva) AS eva,
                    COALESCE(p.train_number, f.train_number) AS train_number,
                    COALESCE(
                        p.arrival_line_number,
                        p.departure_line_number,
                        f.arrival_line_number,
                        f.departure_line_number
                    ) AS line_number,
                    -- Keep the destination unknown when no departure planned path exists.
                    list_extract(COALESCE(p.departure_planned_path, f.departure_planned_path), -1)
                        AS final_destination_station,
                    COALESCE(p.train_type, f.train_type) AS train_type,
                    COALESCE(p.arrival_planned_time, f.arrival_planned_time) AS arrival_planned_time,
                    COALESCE(p.departure_planned_time, f.departure_planned_time) AS departure_planned_time,
                    COALESCE(f.arrival_change_time, p.arrival_planned_time, f.arrival_planned_time)
                        AS arrival_change_time,
                    COALESCE(f.departure_change_time, p.departure_planned_time, f.departure_planned_time)
                        AS departure_change_time,
                    COALESCE(
                        f.arrival_change_status = 'c',
                        f.arrival_cancellation_time IS NOT NULL
                    ) AS arrival_is_canceled,
                    COALESCE(
                        f.departure_change_status = 'c',
                        f.departure_cancellation_time IS NOT NULL
                    ) AS departure_is_canceled,
                    -- ps='a' marks a stop that was not on the scheduled path.
                    COALESCE(f.arrival_planned_status = 'a', false)
                        OR COALESCE(f.departure_planned_status = 'a', false) AS is_additional_stop,
                    COALESCE(f.train_label_type = 'e', false)
                        OR f.replaced_train_type IS NOT NULL
                        OR f.replaced_train_number IS NOT NULL AS is_replacement_train,
                    f.replaced_train_type AS replaced_train_type,
                    f.replaced_train_number AS replaced_train_number,
                    p.id IS NOT NULL AS has_plan_record
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
                    replaced_train_type,
                    replaced_train_number,
                    has_plan_record,
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
        ) TO '{temp_output}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)
    """)

    temp_output.replace(output_file)
    print(f"Saved records to {output_file}")
    print(f"Total processing time: {time.time() - start_time:.2f} seconds")
    return output_file


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: uv run python scripts/create_monthly_data_release.py <year> <month>")
        sys.exit(1)

    input_year = int(sys.argv[1])
    input_month = int(sys.argv[2])
    parsed_dir = Path("monthly_parsed_data")
    main(
        input_year,
        input_month,
        parsed_dir / "plan" / f"data-{input_year}-{input_month:02d}.parquet",
        parsed_dir / "fchg" / f"data-{input_year}-{input_month:02d}.parquet",
        Path("monthly_processed_data"),
    )
