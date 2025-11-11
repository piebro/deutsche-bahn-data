import argparse
import asyncio
import json
import logging
from datetime import datetime

from db_data_fetcher import fetch_and_save

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def fetch_eva_numbers(category: int, parquet_filename: str) -> list[str]:
    queries = [
        {
            "url": "https://apis.deutschebahn.com/db-api-marketplace/apis/station-data/v2/stations",
            "params": {"category": str(category)},
        },
    ]
    df = await fetch_and_save(queries=queries, output_path="raw_data", parquet_filename=parquet_filename)

    eva_numbers = []
    for station in json.loads(df["response_data"].iloc[0])["result"]:
        station_eva_numbers = []
        for eva in station["evaNumbers"]:
            station_eva_numbers.append(f"0{eva.get('number')}")  # add a leading 0 for the eva
        eva_numbers.extend(station_eva_numbers)
    return eva_numbers


async def fetch_changes(eva_numbers: list[str], max_concurrent: int, rate_limit: int, parquet_filename: str) -> None:
    queries = []
    fchg_base = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg"
    for eva in eva_numbers:
        queries.append({"url": f"{fchg_base}/{eva}"})
    logger.info(f"Fetching {len(queries)} changes")
    await fetch_and_save(
        queries=queries,
        output_path="raw_data",
        max_concurrent=max_concurrent,
        rate_limit=rate_limit,
        parquet_filename=parquet_filename,
    )


async def fetch_plan(
    eva_numbers: list[str], date_str: str, hour: int, max_concurrent: int, rate_limit: int, parquet_filename: str
) -> None:
    queries = []
    plan_base = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan"
    for eva in eva_numbers:
        queries.append({"url": f"{plan_base}/{eva}/{date_str}/{hour:02d}"})
    logger.info(f"Fetching {len(queries)} plans for {date_str} at {hour:02d}")
    await fetch_and_save(
        queries=queries,
        output_path="raw_data",
        max_concurrent=max_concurrent,
        rate_limit=rate_limit,
        parquet_filename=parquet_filename,
    )


async def main(categories: list[int], date_str: str, hours: list[int], parquet_filename: str):
    """Main execution function."""
    logger.info(f"Categories: {categories}, Date: {date_str}, Hours: {hours}")

    # save facility data
    await fetch_and_save(
        queries=[{"url": "https://apis.deutschebahn.com/db-api-marketplace/apis/fasta/v2/facilities"}],
        output_path="raw_data",
        parquet_filename=parquet_filename,
    )

    eva_numbers = []
    for category in categories:
        eva_numbers_for_category = await fetch_eva_numbers(category=category, parquet_filename=parquet_filename)
        print(f"Fetched {len(eva_numbers_for_category)} EVA numbers for category {category}")
        eva_numbers.extend(eva_numbers_for_category)

    eva_numbers_to_exclude = [
        "08083368",  # ("Köln Messe/Deutz"), bad request error, probably deprecated
    ]
    eva_numbers = list(set(eva_numbers) - set(eva_numbers_to_exclude))

    await fetch_changes(eva_numbers, max_concurrent=10, rate_limit=1000, parquet_filename=parquet_filename)
    for hour in hours:
        logger.info(f"Fetching plan for {date_str} at {hour:02d}")
        await fetch_plan(
            eva_numbers, date_str, hour, max_concurrent=10, rate_limit=1000, parquet_filename=parquet_filename
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Deutsche Bahn station data, changes, and timetable plans")
    parser.add_argument(
        "--categories", type=str, default="1,2", help="Comma-separated list of station categories (default: 1,2)"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--hours",
        type=str,
        default=str(datetime.now().hour),
        help="Comma-separated list of hours 0-23 (default: current hour)",
    )

    args = parser.parse_args()

    # Parse comma-separated values
    categories = [int(c.strip()) for c in args.categories.split(",")]
    hours = [int(h.strip()) for h in args.hours.split(",")]

    # Create filename with all hours included
    hours_str = "_".join([f"{h:02d}" for h in sorted(hours)])
    parquet_filename = f"hour_{hours_str}.parquet"

    # Convert date from YYYY-MM-DD to YYMMDD format for the API
    date_obj = datetime.strptime(args.date, "%Y-%m-%d")
    date_str = date_obj.strftime("%y%m%d")

    logger.info(f"Categories: {categories}")
    logger.info(f"Date: {date_str}")
    logger.info(f"Hours: {hours}")
    logger.info(f"Parquet filename: {parquet_filename}")
    asyncio.run(main(categories, date_str, hours, parquet_filename))
