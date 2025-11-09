import pandas as pd
import pytest

from scripts.create_monthly_data_release import main


@pytest.mark.parametrize(
    "input_csv_path,expected_csv_path,year,month",
    [
        ("test_scripts/test_data/valid_input.csv", "test_scripts/test_data/valid_expected.csv", 2025, 1),
        ("test_scripts/test_data/edge_cases_input.csv", "test_scripts/test_data/edge_cases_expected.csv", 2025, 1),
    ],
)
def test_main(tmp_path, input_csv_path, expected_csv_path, year, month):
    """End-to-end test for the main function of create_monthly_data_release.py"""

    # Load input CSV with proper dtypes and save it as parquet
    input_df = pd.read_csv(
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
    test_parquet_file = tmp_path / "test_data.parquet"
    input_df.to_parquet(test_parquet_file, index=False)

    # Create minimal eva_to_station dict with only what's needed for the test
    eva_to_station = {"08000105": "Frankfurt (Main) Hbf"}

    # Run main with test data
    main(year, month, [test_parquet_file], eva_to_station, output_dir=tmp_path)

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
            "train_name": str,
            "final_destination_station": str,
            "delay_in_min": "int32",
            "is_canceled": bool,
            "train_type": str,
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

    pd.testing.assert_frame_equal(output_df, expected_df)
