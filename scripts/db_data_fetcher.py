import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiohttp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from aiolimiter import AsyncLimiter
from dotenv import load_dotenv
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


def extract_api_name(url: str) -> str:
    """Extract the API name from a URL."""
    prefix = "https://apis.deutschebahn.com/db-api-marketplace/apis/"

    # Remove query parameters if present
    url_without_params = url.split("?")[0]

    # Remove the prefix if present
    if url_without_params.startswith(prefix):
        path = url_without_params[len(prefix) :]
        # Split by "/" and take the first three parts
        parts = path.split("/")
        if len(parts) >= 3:
            return "/".join(parts[:3])

    # Fallback: use the whole URL
    return url


@dataclass
class QueryResult:
    """Result of a single API query."""

    timestamp: datetime
    url: str
    api_name: str
    query_params: dict[str, Any]
    response_data: str | None
    status_code: int | None
    error: str | None
    duration_ms: float


class _DBApiClient:
    """
    Internal async client for Deutsche Bahn APIs with concurrency control, rate limiting, and retries.
    """

    def __init__(
        self,
        api_key: str,
        client_id: str,
        max_concurrent: int,
        rate_limit: int,
        max_retries: int,
        timeout: int,
    ):
        self.api_key = api_key
        self.client_id = client_id
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.timeout = timeout
        self.retry_on_status = (429, 500, 502, 503, 504)

        # Rate limiter: convert requests per minute to requests per second
        self.rate_limiter = AsyncLimiter(rate_limit, 60)

        # Semaphore for concurrency control
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # Statistics
        self.stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "retried_requests": 0,
        }

    async def fetch_all(
        self,
        queries: list[dict[str, Any]],
        show_progress: bool = True,
    ) -> list[QueryResult]:
        """Fetch all queries concurrently with rate limiting and retries."""
        self.stats = {
            "total_requests": len(queries),
            "successful_requests": 0,
            "failed_requests": 0,
            "retried_requests": 0,
        }

        start_time = asyncio.get_event_loop().time()
        logger.info(f"Starting to fetch {len(queries)} queries with max_concurrent={self.max_concurrent}")

        async with aiohttp.ClientSession(
            headers={
                "DB-Api-Key": self.api_key,
                "DB-Client-Id": self.client_id,
                "Accept": "application/xml, application/json",
            },
            timeout=aiohttp.ClientTimeout(total=self.timeout),
        ) as session:
            tasks = [self._fetch_one(session, query, idx, show_progress) for idx, query in enumerate(queries)]
            results = await asyncio.gather(*tasks, return_exceptions=False)

        elapsed_time = asyncio.get_event_loop().time() - start_time
        logger.info(
            f"Completed fetching in {elapsed_time:.2f}s. Success: {self.stats['successful_requests']}, "
            f"Failed: {self.stats['failed_requests']}, "
            f"Retried: {self.stats['retried_requests']}"
        )

        return results

    async def _fetch_one(
        self,
        session: aiohttp.ClientSession,
        query: dict[str, Any],
        idx: int,
        show_progress: bool,
    ) -> QueryResult:
        """Fetch a single query with rate limiting, concurrency control, and retries."""
        # Extract URL and params from query dict
        url = query["url"]
        params = query.get("params")

        # Build full URL with query parameters if provided
        if params:
            url = f"{url}?{urlencode(params)}"
            query_params = params
        else:
            query_params = {}

        api_name = extract_api_name(url)
        overall_start_time = asyncio.get_event_loop().time()

        async with self.semaphore:  # Control concurrency
            async with self.rate_limiter:  # Control rate
                start_time = asyncio.get_event_loop().time()

                try:
                    result = await self._fetch_with_retry(session, url)
                    duration_ms = (asyncio.get_event_loop().time() - start_time) * 1000

                    self.stats["successful_requests"] += 1

                    if show_progress and (idx + 1) % 100 == 0:
                        elapsed = asyncio.get_event_loop().time() - overall_start_time
                        logger.info(
                            f"Progress: {idx + 1}/{self.stats['total_requests']} queries completed (elapsed: {elapsed:.2f}s)"
                        )

                    return QueryResult(
                        timestamp=datetime.now(),
                        url=url,
                        api_name=api_name,
                        query_params=query_params,
                        response_data=result["data"],
                        status_code=result["status"],
                        error=None,
                        duration_ms=duration_ms,
                    )

                except Exception as e:
                    duration_ms = (asyncio.get_event_loop().time() - start_time) * 1000
                    self.stats["failed_requests"] += 1

                    logger.error(f"Failed to fetch {url}: {e}")

                    return QueryResult(
                        timestamp=datetime.now(),
                        url=url,
                        api_name=api_name,
                        query_params=query_params,
                        response_data=None,
                        status_code=None,
                        error=str(e),
                        duration_ms=duration_ms,
                    )

    async def _fetch_with_retry(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> dict[str, str | int]:
        """Fetch with exponential backoff retry logic."""

        @retry(
            retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=2, min=5, max=30),
            reraise=True,
        )
        async def _fetch():
            async with session.get(url) as response:
                # Retry on specific status codes
                if response.status in self.retry_on_status:
                    self.stats["retried_requests"] += 1
                    raise aiohttp.ClientError(f"HTTP {response.status}: retrying")

                # Raise for other error status codes
                response.raise_for_status()

                data = await response.text()
                return {"data": data, "status": response.status}

        return await _fetch()


def _results_to_dataframe(results: list[QueryResult]) -> pd.DataFrame:
    """Convert a list of QueryResult objects to a pandas DataFrame."""
    data = []
    for result in results:
        data.append(
            {
                "timestamp": result.timestamp,
                "url": result.url,
                "api_name": result.api_name,
                "query_params": json.dumps(result.query_params) if result.query_params else None,
                "response_data": result.response_data,
                "status_code": str(result.status_code) if result.status_code is not None else None,
                "error": result.error,
                "duration_ms": result.duration_ms,
                "year": result.timestamp.year,
                "month": result.timestamp.month,
                "day": result.timestamp.day,
            }
        )

    df = pd.DataFrame(data)

    # Ensure proper data types
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["duration_ms"] = df["duration_ms"].astype("float64")
        df["year"] = df["year"].astype("int32")
        df["month"] = df["month"].astype("int32")
        df["day"] = df["day"].astype("int32")

    return df


def _save_to_parquet(
    df: pd.DataFrame,
    output_path: str | Path,
    parquet_filename: str = "data.parquet",
) -> None:
    """Save DataFrame to a partitioned Parquet dataset."""
    output_path = Path(output_path)

    if df.empty:
        logger.warning("No results to save, skipping parquet write")
        return

    schema = pa.schema(
        [
            ("timestamp", pa.timestamp("us")),
            ("url", pa.string()),
            ("api_name", pa.string()),
            ("query_params", pa.string()),
            ("response_data", pa.string()),
            ("status_code", pa.string()),
            ("error", pa.string()),
            ("duration_ms", pa.float64()),
            ("year", pa.int32()),
            ("month", pa.int32()),
            ("day", pa.int32()),
        ]
    )

    for (year, month, day), partition_df in df.groupby(["year", "month", "day"]):
        partition_dir = output_path / f"year={year}" / f"month={month}" / f"day={day}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        partition_file = partition_dir / parquet_filename

        # Convert partition to PyArrow Table with fixed schema
        table = pa.Table.from_pandas(partition_df, schema=schema)

        if partition_file.exists():
            # Read existing data
            parquet_file = pq.ParquetFile(partition_file)
            existing_table = parquet_file.read()
            # Concatenate with new data (schemas are now guaranteed to match)
            combined_table = pa.concat_tables([existing_table, table])
            # Write back
            pq.write_table(combined_table, partition_file)
            logger.info(f"Appended {len(partition_df)} results to {partition_file}")
        else:
            # Create new file
            pq.write_table(table, partition_file)
            logger.info(f"Created new partition at {partition_file} with {len(partition_df)} results")


async def fetch_and_save(
    queries: list[dict[str, Any]],
    output_path: str | Path,
    max_concurrent: int = 30,
    rate_limit: int = 60,
    max_retries: int = 5,
    timeout: int = 15,
    parquet_filename: str = "data.parquet",
) -> pd.DataFrame:
    """
    Fetch Deutsche Bahn API queries, save to partitioned Parquet, and return as DataFrame.

    This is the main function that handles everything: fetching API data with concurrency control,
    rate limiting, and retries, then saving to a partitioned Parquet dataset and returning the results.

    Args:
        queries: List of query dicts, where each dict contains:
            - url: str - The API endpoint URL
            - params: dict[str, Any] | None - Optional query parameters
        output_path: Base directory path for the partitioned parquet dataset
        max_concurrent: Maximum number of concurrent requests (default: 50)
        rate_limit: Maximum requests per minute (default: 60)
        max_retries: Number of retry attempts for failed requests (default: 5)
        timeout: Request timeout in seconds (default: 15)

    Returns:
        DataFrame with columns: timestamp, url, api_name, query_params, response_data,
                                status_code, error, duration_ms, year, month, day

    Example:
        >>> queries = [
        ...     {"url": "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/station/8000105"},
        ...     {"url": "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/station/8000191"},
        ... ]
        >>> df = await fetch_and_save(
        ...     queries=queries,
        ...     api_key="your_api_key",
        ...     client_id="your_client_id",
        ...     output_path="data/timetables"
        ... )
    """
    load_dotenv()

    api_key = os.getenv("DB_API_KEY")
    client_id = os.getenv("DB_CLIENT_ID")

    if not api_key or not client_id:
        raise ValueError("DB_API_KEY and DB_CLIENT_ID environment variables must be set")

    # Create client and fetch all queries
    client = _DBApiClient(
        api_key=api_key,
        client_id=client_id,
        max_concurrent=max_concurrent,
        rate_limit=rate_limit,
        max_retries=max_retries,
        timeout=timeout,
    )

    results = await client.fetch_all(queries)
    df = _results_to_dataframe(results)
    _save_to_parquet(df, output_path, parquet_filename)
    return df
