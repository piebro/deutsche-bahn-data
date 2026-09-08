from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

REPO_ID = "piebro/deutsche-bahn-data"
BERLIN_TIMEZONE = ZoneInfo("Europe/Berlin")
DATED_FILE_RE = re.compile(r"(?:^|/)date_(\d{4}-\d{2}-\d{2})_hour_((?:\d{2})(?:_\d{2})*)\.parquet$")
LEGACY_FILE_RE = re.compile(r"(?:^|/)year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})/hour_((?:\d{2})(?:_\d{2})*)\.parquet$")

# HTTP statuses worth retrying (transient rate limits / server hiccups).
RETRIABLE_STATUS = {429, 500, 502, 503, 504}


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


def _with_retry(fn, *, retries: int = 5, base_delay: float = 2.0, max_delay: float = 60.0):
    """Call ``fn()``, retrying transient Hugging Face / network errors with exponential backoff."""
    import httpx
    from huggingface_hub.errors import HfHubHTTPError

    last_exc = None
    for attempt in range(retries):
        try:
            return fn()
        except HfHubHTTPError as exc:
            if getattr(exc.response, "status_code", None) not in RETRIABLE_STATUS:
                raise
            last_exc = exc
        except (httpx.HTTPError, OSError) as exc:  # connection resets, DNS, timeouts, ...
            last_exc = exc
        delay = min(base_delay * (2**attempt), max_delay)
        print(
            f"Hugging Face request failed ({last_exc}); retry {attempt + 1}/{retries} in {delay:.0f}s", file=sys.stderr
        )
        time.sleep(delay)
    if last_exc is not None:
        raise last_exc


def _list_folder_paths(api, repo_id: str, folder: str) -> list[str]:
    """Non-recursively list a folder, returning file paths ([] when the folder does not exist)."""
    from huggingface_hub.errors import HfHubHTTPError

    try:
        entries = _with_retry(
            lambda: list(api.list_repo_tree(repo_id=repo_id, repo_type="dataset", path_in_repo=folder, recursive=False))
        )
    except HfHubHTTPError as exc:
        if getattr(exc.response, "status_code", None) == 404:
            return []
        raise
    return [
        entry.path
        for entry in entries
        if entry.path and (DATED_FILE_RE.search(entry.path) or LEGACY_FILE_RE.search(entry.path))
    ]


def relevant_repo_files(api, repo_id: str, expected_hours: set[datetime]) -> list[str]:
    """Scoped-listing alternative to the recursive ``list_repo_files``.

    Only the day folders overlapping the expected repair window are listed (plus the
    ``raw_data/`` root for flat dated files), instead of recursively walking the whole
    dataset. Files are partitioned by fetch-time day, so a Berlin date D file for the
    first hours of the day can live under day=D-1 - list both to be safe.
    """
    paths: set[str] = set()
    day_folders = set()
    for hour_bucket in expected_hours:
        day = hour_bucket.date()
        day_folders.add(day)
        day_folders.add(day - timedelta(days=1))

    for day in sorted(day_folders):
        folder = f"raw_data/year={day.year}/month={day.month}/day={day.day}"
        paths.update(_list_folder_paths(api, repo_id, folder))

    # Flat dated files that used to be stored directly under raw_data/ (older layout).
    paths.update(_list_folder_paths(api, repo_id, "raw_data"))

    return sorted(paths)


def main() -> None:
    from huggingface_hub import HfApi

    parser = argparse.ArgumentParser(description="Find missing hourly raw-data files in a Hugging Face dataset")
    parser.add_argument("--repo-id", default=REPO_ID)
    parser.add_argument("--lookback-hours", type=int, default=20)
    parser.add_argument("--lookahead-hours", type=int, default=0)
    args = parser.parse_args()

    api = HfApi()
    now = datetime.now(BERLIN_TIMEZONE)
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    expected = {
        current_hour + timedelta(hours=offset) for offset in range(-(args.lookback_hours - 1), args.lookahead_hours + 1)
    }

    repo_files = relevant_repo_files(api, args.repo_id, expected)
    groups = missing_hour_groups(
        repo_files=repo_files,
        now=now,
        lookback_hours=args.lookback_hours,
        lookahead_hours=args.lookahead_hours,
    )
    print(json.dumps(groups))


if __name__ == "__main__":
    main()
