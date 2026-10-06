import pandas as pd
import pyarrow.parquet as pq
import pytest

from scripts.create_monthly_data_release import main as create_monthly_data_release
from scripts.parse_monthly_xml import FCHG_SCHEMA, PLAN_SCHEMA
from scripts.parse_monthly_xml import main as parse_monthly_xml


def load_raw_fixture(input_csv_path):
    return pd.read_csv(
        input_csv_path,
        dtype={
            "url": str,
            "api_name": str,
            "query_params": str,
            "response_data": str,
            "status_code": str,
            "error": str,
            "duration_ms": float,
            "year": int,
            "month": int,
            "day": int,
        },
        parse_dates=["timestamp"],
    )


@pytest.mark.parametrize(
    "input_csv_path,expected_csv_path,year,month",
    [
        ("test_scripts/test_data/valid_input.csv", "test_scripts/test_data/valid_expected.csv", 2025, 1),
        ("test_scripts/test_data/edge_cases_input.csv", "test_scripts/test_data/edge_cases_expected.csv", 2025, 1),
        (
            "test_scripts/test_data/additional_stops_input.csv",
            "test_scripts/test_data/additional_stops_expected.csv",
            2025,
            1,
        ),
    ],
)
def test_monthly_pipeline(tmp_path, input_csv_path, expected_csv_path, year, month):
    """Parse raw XML snapshots and build the monthly stop table."""
    input_df = load_raw_fixture(input_csv_path)
    test_parquet_file = tmp_path / "test_data.parquet"
    input_df.to_parquet(test_parquet_file, index=False)

    # Create minimal eva_to_station dict with only what's needed for the test
    eva_to_station = {"08000105": "Frankfurt (Main) Hbf"}

    plan_file, fchg_file = parse_monthly_xml(
        year,
        month,
        [test_parquet_file],
        eva_to_station,
        plan_dir=tmp_path / "monthly_processed_data_plan",
        change_dir=tmp_path / "monthly_processed_data_change",
    )

    assert pq.read_schema(plan_file) == PLAN_SCHEMA
    assert pq.read_schema(fchg_file) == FCHG_SCHEMA

    plan_df = pd.read_parquet(plan_file)
    fchg_df = pd.read_parquet(fchg_file)
    assert len(plan_df) == plan_df["id"].nunique()
    assert len(fchg_df) == fchg_df["id"].nunique()

    if input_csv_path.endswith("additional_stops_input.csv"):
        replacement = fchg_df.set_index("id").loc["REPL-2838-2501151000-1"]
        assert list(replacement["arrival_planned_path"]) == [
            "Koblenz Hbf",
            "Frankfurt (Main) Hbf",
            "Berlin Hbf",
        ]
        assert replacement["train_label_type"] == "e"
        assert replacement["replaced_train_type"] == "ICE"
        assert replacement["replaced_train_number"] == "1678"

    create_monthly_data_release(year, month, plan_file, fchg_file, output_dir=tmp_path)

    # Load the output
    output_file = tmp_path / f"data-{year}-{month:02d}.parquet"
    assert output_file.exists(), f"Output file {output_file} was not created"

    output_df = pd.read_parquet(output_file)

    # Load expected output with proper dtypes
    expected_df = pd.read_csv(
        expected_csv_path,
        dtype={
            "station_name": str,
            "xml_station_name": str,
            "eva": str,
            "train_number": str,
            "line_number": str,
            "final_destination_station": str,
            "delay_in_min": "int32",
            "arrival_is_canceled": bool,
            "departure_is_canceled": bool,
            "train_type": str,
            "is_additional_stop": bool,
            "is_replacement_train": bool,
            "replaced_train_number": str,
            "train_line_ride_id": str,
            "train_line_station_num": "int32",
            "id": str,
        },
        parse_dates=[
            "time",
            "arrival_planned_time",
            "arrival_change_time",
            "departure_planned_time",
            "departure_change_time",
        ],
    )

    # Sort both dataframes by id to ensure consistent ordering
    output_df = output_df.sort_values("id").reset_index(drop=True)
    expected_df = expected_df.sort_values("id").reset_index(drop=True)

    # Normalize None/NaN so nullable string columns compare consistently
    output_df = output_df.where(output_df.notna(), other=None)
    expected_df = expected_df.where(expected_df.notna(), other=None)

    # pandas 3.0 parses CSV timestamps as microseconds and reads ns parquet back as
    # nanoseconds; normalize to a common resolution so only the values are compared.
    for col in [
        "time",
        "arrival_planned_time",
        "arrival_change_time",
        "departure_planned_time",
        "departure_change_time",
    ]:
        output_df[col] = output_df[col].astype("datetime64[ns]")
        expected_df[col] = expected_df[col].astype("datetime64[ns]")

    actual_new_columns = output_df[["id", "replaced_train_type", "has_plan_record"]].copy()
    output_df = output_df.drop(columns=["replaced_train_type", "has_plan_record"])
    pd.testing.assert_frame_equal(output_df, expected_df)

    plan_ids = set(plan_df["id"])
    assert actual_new_columns.set_index("id")["has_plan_record"].to_dict() == {
        stop_id: stop_id in plan_ids for stop_id in actual_new_columns["id"]
    }

    replacement_types = actual_new_columns.set_index("id")["replaced_train_type"].dropna().to_dict()
    if input_csv_path.endswith("additional_stops_input.csv"):
        assert replacement_types == {"REPL-2838-2501151000-1": "ICE"}
    else:
        assert replacement_types == {}


def test_parser_keeps_every_snapshot(tmp_path):
    input_df = load_raw_fixture("test_scripts/test_data/valid_input.csv")
    later_snapshot = input_df.copy()
    later_snapshot["timestamp"] += pd.Timedelta(minutes=10)
    is_fchg = later_snapshot["api_name"] == "timetables/v1/fchg"
    later_snapshot.loc[is_fchg, "response_data"] = later_snapshot.loc[is_fchg, "response_data"].str.replace(
        "2501151050", "2501151055"
    )
    message_snapshot = later_snapshot[is_fchg].copy()
    message_snapshot["timestamp"] += pd.Timedelta(minutes=10)
    message_snapshot["response_data"] = """<timetable station="Frankfurt (Main) Hbf">
        <s id="-7654321000000000001-2501151030-2"><dp l="99"><m id="message"/></dp></s>
    </timetable>"""
    input_df = pd.concat([input_df, later_snapshot, message_snapshot], ignore_index=True)
    test_parquet_file = tmp_path / "snapshots.parquet"
    input_df.to_parquet(test_parquet_file, index=False)

    plan_file, fchg_file = parse_monthly_xml(
        2025,
        1,
        [test_parquet_file],
        {"08000105": "Frankfurt (Main) Hbf"},
        plan_dir=tmp_path / "monthly_processed_data_plan",
        change_dir=tmp_path / "monthly_processed_data_change",
    )

    plan_df = pd.read_parquet(plan_file)
    fchg_df = pd.read_parquet(fchg_file)
    assert len(plan_df) == 4
    assert len(fchg_df) == 5
    assert plan_df.groupby("id")["snapshot_timestamp"].nunique().eq(2).all()
    assert fchg_df.groupby("id")["snapshot_timestamp"].nunique().to_dict() == {
        "-7654321000000000001-2501151030-2": 3,
        "123456789-2501151000-1": 2,
    }

    output_file = create_monthly_data_release(2025, 1, plan_file, fchg_file, output_dir=tmp_path)
    output_df = pd.read_parquet(output_file).set_index("id")
    assert output_df.loc["-7654321000000000001-2501151030-2", "departure_change_time"] == pd.Timestamp(
        "2025-01-15 10:55"
    )


def test_reinstated_stop_is_not_canceled(tmp_path):
    input_df = load_raw_fixture("test_scripts/test_data/edge_cases_input.csv")
    is_fchg = input_df["api_name"] == "timetables/v1/fchg"
    input_df.loc[is_fchg, "response_data"] = input_df.loc[is_fchg, "response_data"].str.replace(
        'ar clt="2501151015"', 'ar clt="2501151015" cs="p"'
    )
    test_parquet_file = tmp_path / "reinstated.parquet"
    input_df.to_parquet(test_parquet_file, index=False)

    plan_file, fchg_file = parse_monthly_xml(
        2025,
        1,
        [test_parquet_file],
        {"08000105": "Frankfurt (Main) Hbf"},
        plan_dir=tmp_path / "monthly_processed_data_plan",
        change_dir=tmp_path / "monthly_processed_data_change",
    )
    output_file = create_monthly_data_release(2025, 1, plan_file, fchg_file, output_dir=tmp_path)
    output_df = pd.read_parquet(output_file).set_index("id")

    assert not output_df.loc["CANCELED-2501151000-1", "arrival_is_canceled"]
