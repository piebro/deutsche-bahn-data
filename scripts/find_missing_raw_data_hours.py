import argparse
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

REPO_ID = "piebro/deutsche-bahn-data"
BERLIN_TIMEZONE = ZoneInfo("Europe/Berlin")
DATED_FILE_RE = re.compile(r"(?:^|/)date_(\d{4}-\d{2}-\d{2})_hour_((?:\d{2})(?:_\d{2})*)\.parquet$")
LEGACY_FILE_RE = re.compile(r"(?:^|/)year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})/hour_((?:\d{2})(?:_\d{2})*)\.parquet$")


def covered_hours(repo_files: list[str]) -> set[datetime]:
    """Return Berlin-time hour buckets represented by raw-data parquet filenames."""
    covered = set()

    for path in repo_files:
        match = DATED_FILE_RE.search(path)
        if match:
            date = datetime.strptime(match.group(1), "%Y-%m-%d").replace(tzinfo=BERLIN_TIMEZONE)
            hours = match.group(2)
        else:
            match = LEGACY_FILE_RE.search(path)
            if not match:
                continue
            date = datetime(
                int(match.group(1)),
                int(match.group(2)),
                int(match.group(3)),
                tzinfo=BERLIN_TIMEZONE,
            )
            hours = match.group(4)

        for hour in hours.split("_"):
            covered.add(date.replace(hour=int(hour)))

    return covered


def missing_hour_groups(
    repo_files: list[str],
    now: datetime,
    lookback_hours: int,
    lookahead_hours: int = 0,
) -> list[dict[str, str | list[int]]]:
    """Group missing Berlin-time hour buckets by date for efficient API fetching."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if lookback_hours < 1:
        raise ValueError("lookback_hours must be at least 1")
    if lookahead_hours < 0:
        raise ValueError("lookahead_hours must be at least 0")

    current_hour = now.astimezone(BERLIN_TIMEZONE).replace(minute=0, second=0, microsecond=0)
    expected = {current_hour + timedelta(hours=offset) for offset in range(-(lookback_hours - 1), lookahead_hours + 1)}
    missing = sorted(expected - covered_hours(repo_files))

    grouped: dict[str, list[int]] = defaultdict(list)
    for hour_bucket in missing:
        grouped[hour_bucket.strftime("%Y-%m-%d")].append(hour_bucket.hour)

    return [{"date": date, "hours": hours} for date, hours in sorted(grouped.items())]


def main() -> None:
    from huggingface_hub import HfApi

    parser = argparse.ArgumentParser(description="Find missing hourly raw-data files in a Hugging Face dataset")
    parser.add_argument("--repo-id", default=REPO_ID)
    parser.add_argument("--lookback-hours", type=int, default=20)
    parser.add_argument("--lookahead-hours", type=int, default=0)
    args = parser.parse_args()

    repo_files = HfApi().list_repo_files(repo_id=args.repo_id, repo_type="dataset")
    groups = missing_hour_groups(
        repo_files=repo_files,
        now=datetime.now(BERLIN_TIMEZONE),
        lookback_hours=args.lookback_hours,
        lookahead_hours=args.lookahead_hours,
    )
    print(json.dumps(groups))


if __name__ == "__main__":
    main()
