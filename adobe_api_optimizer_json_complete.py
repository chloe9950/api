"""
Optimized Adobe Marketing API Client for Databricks
Handles rate limiting (500 calls/min) and payload size limits (1MB) efficiently
"""

import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore, Lock
from typing import List, Dict, Any
import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_json, struct, size as spark_size
from datetime import datetime, timedelta


class RateLimitedAdobeClient:
    """
    Adobe API client with intelligent rate limiting and payload management
    """

    def __init__(
        self,
        api_endpoint: str,
        headers: Dict[str, str],
        max_calls_per_minute: int = 500,
        max_payload_size_mb: float = 1.0,
        max_workers: int = 50  # Concurrent threads
    ):
        self.api_endpoint = api_endpoint
        self.headers = headers
        self.max_calls_per_minute = max_calls_per_minute
        self.max_payload_size_bytes = int(max_payload_size_mb * 1024 * 1024)
        self.max_workers = max_workers

        # Rate limiting infrastructure
        self.semaphore = Semaphore(max_workers)
        self.lock = Lock()
        self.call_timestamps = []

        # Results tracking
        self.success_count = 0
        self.error_count = 0
        self.errors = []

    def _wait_for_rate_limit(self):
        """
        Smart rate limiting: only wait if we're approaching the limit
        """
        with self.lock:
            now = datetime.now()
            # Remove timestamps older than 1 minute
            self.call_timestamps = [
                ts for ts in self.call_timestamps
                if now - ts < timedelta(seconds=60)
            ]

            # If we've hit the limit, wait until the oldest call expires
            if len(self.call_timestamps) >= self.max_calls_per_minute:
                oldest_call = self.call_timestamps[0]
                wait_time = 60 - (now - oldest_call).total_seconds()
                if wait_time > 0:
                    time.sleep(wait_time + 0.1)  # Small buffer
                    # Clean up again after waiting
                    now = datetime.now()
                    self.call_timestamps = [
                        ts for ts in self.call_timestamps
                        if now - ts < timedelta(seconds=60)
                    ]

            # Record this call
            self.call_timestamps.append(datetime.now())

    def _send_single_request(self, payload: Dict[str, Any], index: int) -> Dict[str, Any]:
        """
        Send a single API request with rate limiting
        """
        with self.semaphore:
            try:
                # Wait for rate limit
                self._wait_for_rate_limit()

                # Make the API call
                response = requests.post(
                    self.api_endpoint,
                    headers=self.headers,
                    json=payload,
                    timeout=30
                )

                response.raise_for_status()

                with self.lock:
                    self.success_count += 1

                return {
                    "index": index,
                    "status": "success",
                    "status_code": response.status_code,
                    "response": response.json() if response.content else None
                }

            except requests.exceptions.RequestException as e:
                with self.lock:
                    self.error_count += 1
                    self.errors.append({
                        "index": index,
                        "error": str(e),
                        "payload_preview": str(payload)[:200]
                    })

                return {
                    "index": index,
                    "status": "error",
                    "error": str(e)
                }

    def send_batch(self, payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Send multiple payloads in parallel with rate limiting
        """
        print(f"Starting batch send of {len(payloads)} requests...")
        print(f"Max concurrent workers: {self.max_workers}")
        print(f"Rate limit: {self.max_calls_per_minute} calls/minute")

        start_time = time.time()
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_index = {
                executor.submit(self._send_single_request, payload, idx): idx
                for idx, payload in enumerate(payloads)
            }

            # Process completed tasks
            for i, future in enumerate(as_completed(future_to_index), 1):
                result = future.result()
                results.append(result)

                # Progress update every 50 requests
                if i % 50 == 0 or i == len(payloads):
                    elapsed = time.time() - start_time
                    rate = i / elapsed if elapsed > 0 else 0
                    print(f"Progress: {i}/{len(payloads)} requests completed "
                          f"({rate:.1f} req/s, Success: {self.success_count}, "
                          f"Errors: {self.error_count})")

        elapsed_time = time.time() - start_time
        print(f"\nBatch complete!")
        print(f"Total time: {elapsed_time:.2f} seconds")
        print(f"Average rate: {len(payloads)/elapsed_time:.1f} requests/second")
        print(f"Success: {self.success_count}, Errors: {self.error_count}")

        return results


def prepare_payloads_from_delta(
    df: DataFrame,
    max_payload_size_bytes: int = 1024 * 1024,
    max_rows_per_payload: int = 1000
) -> List[Dict[str, Any]]:
    """
    Convert Delta table to JSON payloads with size and row limits

    Args:
        df: Spark DataFrame from Delta table
        max_payload_size_bytes: Maximum size per payload (default 1MB)
        max_rows_per_payload: Maximum rows per payload for safety

    Returns:
        List of payload dictionaries ready to send
    """
    print("Converting Delta table to JSON payloads...")

    # Convert DataFrame to JSON strings (one per row)
    json_rdd = df.toJSON()
    rows = json_rdd.collect()

    print(f"Total rows to process: {len(rows)}")

    payloads = []
    current_batch = []
    current_size = 0

    for row_json in rows:
        row_dict = json.loads(row_json)
        row_size = len(row_json.encode('utf-8'))

        # Check if adding this row exceeds limits
        if (current_size + row_size > max_payload_size_bytes or
            len(current_batch) >= max_rows_per_payload):

            # Save current batch if it has data
            if current_batch:
                payloads.append({"data": current_batch})
                current_batch = []
                current_size = 0

        current_batch.append(row_dict)
        current_size += row_size

    # Add remaining rows
    if current_batch:
        payloads.append({"data": current_batch})

    print(f"Created {len(payloads)} payloads")
    if payloads:
        avg_size = sum(len(json.dumps(p).encode('utf-8')) for p in payloads) / len(payloads)
        print(f"Average payload size: {avg_size/1024:.2f} KB")

    return payloads


# Example usage in Databricks notebook
def main_example():
    """
    Example usage for Databricks notebook
    """

    # ========================================
    # CONFIGURATION - Update these values
    # ========================================

    ADOBE_API_ENDPOINT = "https://your-adobe-api-endpoint.com/api/v1/data"
    ADOBE_API_KEY = "your-api-key-here"
    DELTA_TABLE_PATH = "/path/to/your/delta/table"

    # API Headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {ADOBE_API_KEY}",
        "x-api-key": ADOBE_API_KEY
    }

    # ========================================
    # STEP 1: Read Delta Table
    # ========================================

    print("Reading Delta table...")
    df = spark.read.format("delta").load(DELTA_TABLE_PATH)

    # Optional: Apply filters or transformations
    # df = df.filter(col("date") == "2024-01-01")
    # df = df.select("col1", "col2", "col3")

    print(f"Total rows in Delta table: {df.count()}")

    # ========================================
    # STEP 2: Prepare Payloads
    # ========================================

    payloads = prepare_payloads_from_delta(
        df,
        max_payload_size_bytes=1024 * 1024,  # 1MB
        max_rows_per_payload=1000
    )

    # ========================================
    # STEP 3: Send to Adobe API
    # ========================================

    client = RateLimitedAdobeClient(
        api_endpoint=ADOBE_API_ENDPOINT,
        headers=headers,
        max_calls_per_minute=500,
        max_payload_size_mb=1.0,
        max_workers=50  # Adjust based on cluster size
    )

    results = client.send_batch(payloads)

    # ========================================
    # STEP 4: Handle Results
    # ========================================

    # Check for errors
    if client.error_count > 0:
        print(f"\n⚠️  {client.error_count} requests failed")
        print("First 5 errors:")
        for error in client.errors[:5]:
            print(f"  - Index {error['index']}: {error['error']}")

    # Save results for audit
    results_df = spark.createDataFrame(results)
    results_df.write.format("delta").mode("overwrite").save("/path/to/results/delta/table")

    print("\n✅ Process complete!")

    return results


# Alternative: Partition-based processing for very large datasets
def process_large_delta_table_partitioned(
    delta_table_path: str,
    partition_column: str,
    api_endpoint: str,
    headers: Dict[str, str]
):
    """
    For very large tables: process partition by partition
    This is more memory efficient
    """

    df = spark.read.format("delta").load(delta_table_path)

    # Get distinct partition values
    partitions = df.select(partition_column).distinct().collect()

    print(f"Processing {len(partitions)} partitions...")

    client = RateLimitedAdobeClient(
        api_endpoint=api_endpoint,
        headers=headers,
        max_calls_per_minute=500,
        max_workers=50
    )

    for partition in partitions:
        partition_value = partition[0]
        print(f"\n--- Processing partition: {partition_column}={partition_value} ---")

        # Read partition
        partition_df = df.filter(col(partition_column) == partition_value)

        # Prepare and send
        payloads = prepare_payloads_from_delta(partition_df)
        results = client.send_batch(payloads)

        print(f"Partition {partition_value} complete")


if __name__ == "__main__":
    # Run the example
    # In Databricks, you would call this from a notebook cell
    main_example()
