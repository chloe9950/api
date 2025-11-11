"""
Adobe Marketing API Sender using Spark Structured Streaming with foreachBatch
Optimized for 500 calls/min rate limit and 1MB payload size
"""

import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore, Lock
from typing import Dict, Any, List
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_json, struct, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


class RateLimitedAPIManager:
    """
    Singleton rate limiter that persists across foreachBatch calls
    """
    _instance = None
    _lock = Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.call_timestamps = []
        self.timestamp_lock = Lock()
        self.success_count = 0
        self.error_count = 0
        self.errors = []
        self._initialized = True

    def wait_for_rate_limit(self, max_calls_per_minute: int = 500):
        """Thread-safe rate limiting"""
        with self.timestamp_lock:
            now = datetime.now()

            # Clean old timestamps
            self.call_timestamps = [
                ts for ts in self.call_timestamps
                if now - ts < timedelta(seconds=60)
            ]

            # Wait if at limit
            if len(self.call_timestamps) >= max_calls_per_minute:
                oldest_call = self.call_timestamps[0]
                wait_time = 60 - (now - oldest_call).total_seconds()
                if wait_time > 0:
                    time.sleep(wait_time + 0.1)
                    now = datetime.now()
                    self.call_timestamps = [
                        ts for ts in self.call_timestamps
                        if now - ts < timedelta(seconds=60)
                    ]

            # Record call
            self.call_timestamps.append(datetime.now())

    def record_success(self):
        with self.timestamp_lock:
            self.success_count += 1

    def record_error(self, error_info: Dict[str, Any]):
        with self.timestamp_lock:
            self.error_count += 1
            self.errors.append(error_info)

    def get_stats(self) -> Dict[str, Any]:
        with self.timestamp_lock:
            return {
                "success": self.success_count,
                "errors": self.error_count,
                "total": self.success_count + self.error_count,
                "success_rate": (self.success_count / (self.success_count + self.error_count) * 100)
                if (self.success_count + self.error_count) > 0 else 0
            }

    def reset_stats(self):
        with self.timestamp_lock:
            self.success_count = 0
            self.error_count = 0
            self.errors = []


class AdobeAPIWriter:
    """
    Spark foreachBatch writer for Adobe API
    """

    def __init__(
        self,
        api_endpoint: str,
        headers: Dict[str, str],
        max_calls_per_minute: int = 500,
        max_payload_size_bytes: int = 1024 * 1024,
        max_workers: int = 50,
        retry_attempts: int = 3,
        batch_rows_per_payload: int = 100
    ):
        self.api_endpoint = api_endpoint
        self.headers = headers
        self.max_calls_per_minute = max_calls_per_minute
        self.max_payload_size_bytes = max_payload_size_bytes
        self.max_workers = max_workers
        self.retry_attempts = retry_attempts
        self.batch_rows_per_payload = batch_rows_per_payload

        # Singleton rate limiter
        self.rate_limiter = RateLimitedAPIManager()

    def send_request(
        self,
        payload: Dict[str, Any],
        payload_id: str,
        attempt: int = 1
    ) -> Dict[str, Any]:
        """Send single request with rate limiting and retry"""
        try:
            # Wait for rate limit
            self.rate_limiter.wait_for_rate_limit(self.max_calls_per_minute)

            # Send request
            response = requests.post(
                self.api_endpoint,
                headers=self.headers,
                json=payload,
                timeout=30
            )

            # Handle rate limit or server errors with retry
            if response.status_code in [429, 500, 502, 503, 504]:
                if attempt < self.retry_attempts:
                    retry_delay = 2 ** (attempt - 1)  # Exponential backoff
                    time.sleep(retry_delay)
                    return self.send_request(payload, payload_id, attempt + 1)

            response.raise_for_status()
            self.rate_limiter.record_success()

            return {
                "payload_id": payload_id,
                "status": "success",
                "status_code": response.status_code,
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            # Retry on exceptions
            if attempt < self.retry_attempts:
                retry_delay = 2 ** (attempt - 1)
                time.sleep(retry_delay)
                return self.send_request(payload, payload_id, attempt + 1)

            # Record error
            error_info = {
                "payload_id": payload_id,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "attempts": attempt
            }
            self.rate_limiter.record_error(error_info)

            return {
                "payload_id": payload_id,
                "status": "error",
                "error": str(e),
                "attempts": attempt
            }

    def chunk_dataframe_to_payloads(self, df: DataFrame) -> List[Dict[str, Any]]:
        """
        Convert DataFrame batch to payloads respecting size limits
        """
        # Convert to JSON rows
        rows = df.toJSON().collect()

        payloads = []
        current_batch = []
        current_size = 0
        batch_id = int(time.time() * 1000)

        for idx, row_json in enumerate(rows):
            row_dict = json.loads(row_json)
            row_size = len(row_json.encode('utf-8'))

            # Check limits
            if (current_size + row_size > self.max_payload_size_bytes or
                len(current_batch) >= self.batch_rows_per_payload):

                if current_batch:
                    payloads.append({
                        'id': f'batch_{batch_id}_chunk_{len(payloads)}',
                        'data': current_batch
                    })
                    current_batch = []
                    current_size = 0

            current_batch.append(row_dict)
            current_size += row_size

        # Add remaining
        if current_batch:
            payloads.append({
                'id': f'batch_{batch_id}_chunk_{len(payloads)}',
                'data': current_batch
            })

        return payloads

    def process_batch(self, df: DataFrame, batch_id: int):
        """
        foreachBatch function to process each micro-batch
        """
        if df.isEmpty():
            print(f"Batch {batch_id}: Empty batch, skipping")
            return

        start_time = time.time()
        row_count = df.count()

        print(f"\n{'='*80}")
        print(f"🔄 Processing Batch {batch_id}")
        print(f"   Rows in batch: {row_count}")
        print(f"   Timestamp: {datetime.now().isoformat()}")

        # Convert to payloads
        payloads = self.chunk_dataframe_to_payloads(df)
        print(f"   Created {len(payloads)} API payloads")

        if not payloads:
            print(f"   No payloads to send")
            return

        # Send in parallel
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_payload = {
                executor.submit(
                    self.send_request,
                    payload['data'],
                    payload['id']
                ): payload['id']
                for payload in payloads
            }

            for i, future in enumerate(as_completed(future_to_payload), 1):
                result = future.result()
                results.append(result)

                # Progress
                if i % 20 == 0 or i == len(payloads):
                    elapsed = time.time() - start_time
                    rate = i / elapsed if elapsed > 0 else 0
                    print(f"   Progress: {i}/{len(payloads)} payloads | Rate: {rate:.1f} req/s")

        elapsed_time = time.time() - start_time
        stats = self.rate_limiter.get_stats()

        print(f"\n   ✅ Batch {batch_id} Complete")
        print(f"   Duration: {elapsed_time:.2f}s | Avg rate: {len(payloads)/elapsed_time:.1f} req/s")
        print(f"   Cumulative stats - Success: {stats['success']} | Errors: {stats['errors']} | "
              f"Rate: {stats['success_rate']:.2f}%")
        print(f"{'='*80}\n")


# =============================================================================
# DATABRICKS NOTEBOOK USAGE - Structured Streaming
# =============================================================================

"""
# ============================================================================
# Cell 1: Install Dependencies & Import
# ============================================================================

# Uncomment if requests is not installed
# %pip install requests

from adobe_api_spark_streaming import AdobeAPIWriter, RateLimitedAPIManager
from pyspark.sql.functions import *
from pyspark.sql.types import *


# ============================================================================
# Cell 2: Configuration
# ============================================================================

# Adobe API Configuration
ADOBE_API_ENDPOINT = "https://api.adobe.io/data/collection/endpoint"
ADOBE_ORG_ID = "your-org-id@AdobeOrg"
ADOBE_API_KEY = "your-api-key"
ADOBE_ACCESS_TOKEN = "your-access-token"

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "x-api-key": ADOBE_API_KEY,
    "x-gw-ims-org-id": ADOBE_ORG_ID,
    "Authorization": f"Bearer {ADOBE_ACCESS_TOKEN}"
}

# Streaming source configuration
INPUT_PATH = "dbfs:/mnt/data/json_files/"
CHECKPOINT_PATH = "dbfs:/mnt/checkpoints/adobe_api/"
OUTPUT_PATH = "dbfs:/mnt/data/adobe_api_results/"

# Performance settings
MAX_WORKERS = 50
MAX_CALLS_PER_MINUTE = 500
MAX_PAYLOAD_SIZE_BYTES = 1024 * 1024  # 1MB
BATCH_ROWS_PER_PAYLOAD = 100  # Rows per API call
TRIGGER_INTERVAL = "30 seconds"  # Process every 30 seconds


# ============================================================================
# Cell 3: Initialize API Writer
# ============================================================================

api_writer = AdobeAPIWriter(
    api_endpoint=ADOBE_API_ENDPOINT,
    headers=headers,
    max_calls_per_minute=MAX_CALLS_PER_MINUTE,
    max_payload_size_bytes=MAX_PAYLOAD_SIZE_BYTES,
    max_workers=MAX_WORKERS,
    retry_attempts=3,
    batch_rows_per_payload=BATCH_ROWS_PER_PAYLOAD
)

print("✅ Adobe API Writer initialized")


# ============================================================================
# Cell 4: Read JSON Files as Stream
# ============================================================================

# Define schema (adjust based on your JSON structure)
json_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("properties", MapType(StringType(), StringType()), True)
    # Add more fields as needed
])

# Read JSON files as streaming DataFrame
streaming_df = (
    spark.readStream
    .format("json")
    .schema(json_schema)
    .option("maxFilesPerTrigger", 10)  # Process 10 files per trigger
    .load(INPUT_PATH)
)

print("✅ Streaming source configured")


# ============================================================================
# Cell 5: Start Streaming with foreachBatch
# ============================================================================

# Option 1: Direct foreachBatch (no output to Delta)
streaming_query = (
    streaming_df
    .writeStream
    .foreachBatch(api_writer.process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

print(f"✅ Streaming job started")
print(f"   Query ID: {streaming_query.id}")
print(f"   Status: {streaming_query.status}")


# ============================================================================
# Cell 6: Monitor Stream (run in separate cell)
# ============================================================================

# Check streaming status
print("Current streaming status:")
print(f"   Active: {streaming_query.isActive}")
print(f"   Status: {streaming_query.status}")

# Get statistics
stats = api_writer.rate_limiter.get_stats()
print(f"\n📊 Cumulative API Statistics:")
print(f"   Total requests: {stats['total']}")
print(f"   Successful: {stats['success']}")
print(f"   Errors: {stats['errors']}")
print(f"   Success rate: {stats['success_rate']:.2f}%")

# Check last progress
if streaming_query.lastProgress:
    progress = streaming_query.lastProgress
    print(f"\n📈 Last Batch Progress:")
    print(f"   Batch ID: {progress.get('batchId')}")
    print(f"   Input rows: {progress.get('numInputRows')}")
    print(f"   Processing time: {progress.get('durationMs', {}).get('triggerExecution')} ms")


# ============================================================================
# Cell 7: Stop Stream (when done)
# ============================================================================

# Stop the stream
streaming_query.stop()
print("✅ Streaming job stopped")

# Final statistics
final_stats = api_writer.rate_limiter.get_stats()
print(f"\n📊 Final Statistics:")
print(f"   Total requests: {final_stats['total']}")
print(f"   Successful: {final_stats['success']}")
print(f"   Errors: {final_stats['errors']}")
print(f"   Success rate: {final_stats['success_rate']:.2f}%")
"""


# =============================================================================
# ALTERNATIVE: Process Existing Delta Table with Streaming
# =============================================================================

"""
# ============================================================================
# Alternative Cell 4: Read from Delta Table as Stream
# ============================================================================

streaming_df = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 100)
    .table("your_delta_table_name")
    # OR
    # .load("dbfs:/mnt/data/your_delta_table/")
)

# Optional: Add filters or transformations
streaming_df = streaming_df.filter(col("event_date") >= "2024-01-01")

print("✅ Delta streaming source configured")
"""


# =============================================================================
# ALTERNATIVE: Auto-loader for JSON files (Recommended for production)
# =============================================================================

"""
# ============================================================================
# Alternative Cell 4: Use Auto Loader (Cloud Files)
# ============================================================================

streaming_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.maxFilesPerTrigger", 10)
    .load(INPUT_PATH)
)

print("✅ Auto Loader configured")
"""


# =============================================================================
# ALTERNATIVE: foreachBatch with output to Delta for audit
# =============================================================================

def process_batch_with_output(batch_df: DataFrame, batch_id: int):
    """
    Process batch and write results to Delta table
    """
    if batch_df.isEmpty():
        return

    # Send to API
    api_writer.process_batch(batch_df, batch_id)

    # Write original data to Delta for audit
    (
        batch_df
        .withColumn("batch_id", lit(batch_id))
        .withColumn("processed_at", current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .save("dbfs:/mnt/data/processed_records/")
    )


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_all_active_streams(spark):
    """Get all active streaming queries"""
    active_streams = spark.streams.active
    print(f"Active streams: {len(active_streams)}")
    for stream in active_streams:
        print(f"  - ID: {stream.id}, Name: {stream.name}, Status: {stream.status}")
    return active_streams


def stop_all_streams(spark):
    """Stop all active streaming queries"""
    for stream in spark.streams.active:
        print(f"Stopping stream: {stream.id}")
        stream.stop()
    print("✅ All streams stopped")


def reset_checkpoint(checkpoint_path: str):
    """
    Reset checkpoint to reprocess data
    WARNING: Use with caution in production
    """
    dbutils.fs.rm(checkpoint_path, recurse=True)
    print(f"✅ Checkpoint deleted: {checkpoint_path}")


# =============================================================================
# PERFORMANCE TUNING TIPS
# =============================================================================

"""
Performance Optimization Tips:

1. Cluster Configuration:
   - Use Standard_DS3_v2 or larger workers
   - Enable autoscaling: 2-10 workers
   - Use Delta Cache for better performance

2. Streaming Configuration:
   spark.conf.set("spark.sql.streaming.schemaInference", "true")
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

3. Rate Limiting:
   - max_workers=50 is optimal for 500 calls/min
   - Increase to 100 if you have larger cluster
   - Decrease to 25 for smaller clusters

4. Batch Size:
   - maxFilesPerTrigger: Adjust based on file size
   - BATCH_ROWS_PER_PAYLOAD: 50-200 depending on row size
   - TRIGGER_INTERVAL: 30-60 seconds for steady processing

5. Monitoring:
   - Use Spark UI to monitor streaming performance
   - Check "Streaming" tab for batch duration
   - Monitor API rate limiter statistics

6. Error Handling:
   - Failed records are logged in rate_limiter.errors
   - Consider implementing DLQ (Dead Letter Queue) for persistent failures
   - Use Delta tables to track processed records
"""


if __name__ == "__main__":
    print("Adobe API Spark Streaming Module - Ready for Databricks!")
    print("\nKey Features:")
    print("✅ Spark Structured Streaming with foreachBatch")
    print("✅ Intelligent rate limiting (500 calls/min)")
    print("✅ Parallel request execution")
    print("✅ Automatic retry with exponential backoff")
    print("✅ Payload size validation (1MB)")
    print("✅ Real-time progress monitoring")
    print("\nCopy the notebook cells to your Databricks notebook to get started!")
