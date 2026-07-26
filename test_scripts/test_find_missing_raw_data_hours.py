from datetime import UTC, datetime

import pytest

from scripts.find_missing_raw_data_hours import (
    BERLIN_TIMEZONE,
    covered_hours,
    missing_hour_groups,
)


def test_covered_hours_supports_dated_and_legacy_filenames():
    files = [
        "raw_data/year=2026/month=7/day=25/hour_22_23.parquet",
        "raw_data/year=2026/month=7/day=26/date_2026-07-26_hour_00_01.parquet",
        "monthly_processed_data/data-2026-06.parquet",
    ]

    assert covered_hours(files) == {
        datetime(2026, 7, 25, 22, tzinfo=BERLIN_TIMEZONE),
        datetime(2026, 7, 25, 23, tzinfo=BERLIN_TIMEZONE),
        datetime(2026, 7, 26, 0, tzinfo=BERLIN_TIMEZONE),
        datetime(2026, 7, 26, 1, tzinfo=BERLIN_TIMEZONE),
    }


def test_missing_hours_are_grouped_across_midnight():
    files = [
        "raw_data/year=2026/month=7/day=25/hour_22_23.parquet",
        "raw_data/year=2026/month=7/day=26/date_2026-07-26_hour_01.parquet",
    ]

    assert missing_hour_groups(
        files,
        now=datetime(2026, 7, 26, 1, 42, tzinfo=BERLIN_TIMEZONE),
        lookback_hours=4,
    ) == [{"date": "2026-07-26", "hours": [0]}]


def test_missing_hours_include_lookahead_across_midnight():
    files = [
        "raw_data/year=2026/month=7/day=26/date_2026-07-26_hour_22_23.parquet",
        "raw_data/year=2026/month=7/day=27/date_2026-07-27_hour_01.parquet",
    ]

    assert missing_hour_groups(
        files,
        # 21:42 UTC is 23:42 in Berlin, so the lookahead crosses midnight.
        now=datetime(2026, 7, 26, 21, 42, tzinfo=UTC),
        lookback_hours=2,
        lookahead_hours=2,
    ) == [{"date": "2026-07-27", "hours": [0]}]


def test_missing_hours_validates_arguments():
    with pytest.raises(ValueError, match="timezone-aware"):
        missing_hour_groups([], datetime(2026, 7, 26), 20)

    with pytest.raises(ValueError, match="at least 1"):
        missing_hour_groups([], datetime(2026, 7, 26, tzinfo=UTC), 0)

    with pytest.raises(ValueError, match="at least 0"):
        missing_hour_groups(
            [],
            datetime(2026, 7, 26, tzinfo=UTC),
            20,
            lookahead_hours=-1,
        )
