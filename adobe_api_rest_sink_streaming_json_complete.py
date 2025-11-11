"""
Adobe Marketing API Sink for Databricks - High Performance REST API Destination
Based on: https://www.databricks.com/blog/scalable-spark-structured-streaming-rest-api-destinations

Optimized for:
- 500 calls per minute rate limit
- 1MB payload size limit
- Maximum throughput with distributed processing
"""

import requests
import json
import time
from typing import Iterator, Dict, Any, List
from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, to_json, struct, current_timestamp, lit,
    monotonically_increasing_id, spark_partition_id,
    row_number, collect_list, size as spark_size,
    length, sum as spark_sum, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, TimestampType, LongType
)
from pyspark.sql.window import Window
import hashlib


class DistributedRateLimiter:
    """
    Distributed rate limiter using partition-based token bucket algorithm
    Each partition gets a fair share of the rate limit
    """

    def __init__(self, max_calls_per_minute: int = 500):
        self.max_calls_per_minute = max_calls_per_minute
        self.tokens_per_second = max_calls_per_minute / 60.0
        self.last_call_time = {}

    def acquire(self, partition_id: int, num_partitions: int):
        """
        Acquire permission to make a call
        Each partition gets equal share of rate limit
        """
        # Calculate this partition's rate limit
        partition_rate = self.tokens_per_second / num_partitions

        # Calculate minimum time between calls for this partition
        min_interval = 1.0 / partition_rate if partition_rate > 0 else 0

        # Get last call time for this partition
        last_time = self.last_call_time.get(partition_id, 0)
        current_time = time.time()

        # Calculate wait time
        elapsed = current_time - last_time
        if elapsed < min_interval:
            wait_time = min_interval - elapsed
            time.sleep(wait_time)
            current_time = time.time()

        # Update last call time
        self.last_call_time[partition_id] = current_time


def send_to_adobe_api(
    iterator: Iterator[Dict[str, Any]],
    api_endpoint: str,
    headers: Dict[str, str],
    max_calls_per_minute: int,
    max_payload_size_bytes: int,
    retry_attempts: int = 3
) -> Iterator[Dict[str, Any]]:
    """
    Process partition with rate limiting and send to Adobe API
    This function runs on each executor

    Args:
        iterator: Iterator of rows in this partition
        api_endpoint: Adobe API endpoint
        headers: HTTP headers
        max_calls_per_minute: Global rate limit
        max_payload_size_bytes: Max payload size
        retry_attempts: Number of retry attempts

    Yields:
        Result dictionaries with status and metadata
    """
    import requests
    import json
    import time
    from datetime import datetime

    # Get partition info
    rows = list(iterator)
    if not rows:
        return

    partition_id = rows[0].get('partition_id', 0)
    num_partitions = rows[0].get('num_partitions', 1)

    # Initialize rate limiter for this partition
    rate_limiter = DistributedRateLimiter(max_calls_per_minute)

    # Group rows into payloads respecting size limit
    payloads = []
    current_payload = []
    current_size = 0

    for row in rows:
        # Remove metadata fields
        data = {k: v for k, v in row.items()
                if k not in ['partition_id', 'num_partitions', 'row_id']}

        row_json = json.dumps(data)
        row_size = len(row_json.encode('utf-8'))

        # Check if adding this row exceeds size limit
        if current_size + row_size > max_payload_size_bytes and current_payload:
            payloads.append(current_payload)
            current_payload = []
            current_size = 0

        current_payload.append(data)
        current_size += row_size

    # Add remaining rows
    if current_payload:
        payloads.append(current_payload)

    # Send each payload
    for payload_idx, payload_data in enumerate(payloads):
        # Acquire rate limit token
        rate_limiter.acquire(partition_id, num_partitions)

        # Prepare payload
        payload = {"data": payload_data}
        payload_id = f"p{partition_id}_batch{payload_idx}"

        # Send with retry
        success = False
        last_error = None

        for attempt in range(1, retry_attempts + 1):
            try:
                response = requests.post(
                    api_endpoint,
                    headers=headers,
                    json=payload,
                    timeout=30
                )

                # Handle rate limit with retry
                if response.status_code == 429:
                    if attempt < retry_attempts:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue

                response.raise_for_status()
                success = True

                yield {
                    "payload_id": payload_id,
                    "partition_id": partition_id,
                    "status": "success",
                    "status_code": response.status_code,
                    "rows_sent": len(payload_data),
                    "payload_size_bytes": len(json.dumps(payload).encode('utf-8')),
                    "timestamp": datetime.now().isoformat(),
                    "attempts": attempt
                }
                break

            except Exception as e:
                last_error = str(e)
                if attempt < retry_attempts:
                    time.sleep(2 ** attempt)
                    continue

        # Record error if all attempts failed
        if not success:
            yield {
                "payload_id": payload_id,
                "partition_id": partition_id,
                "status": "error",
                "error": last_error,
                "rows_sent": len(payload_data),
                "timestamp": datetime.now().isoformat(),
                "attempts": retry_attempts
            }


def prepare_dataframe_for_api(
    df: DataFrame,
    num_partitions: int = 100
) -> DataFrame:
    """
    Prepare DataFrame for distributed API calls

    Args:
        df: Source DataFrame from Delta table
        num_partitions: Number of partitions for parallel processing

    Returns:
        DataFrame with partition metadata
    """
    # Add row ID and partition metadata
    df_with_metadata = (
        df
        .withColumn("row_id", monotonically_increasing_id())
        .repartition(num_partitions)
        .withColumn("partition_id", spark_partition_id())
        .withColumn("num_partitions", lit(num_partitions))
    )

    return df_with_metadata


def send_delta_to_adobe_api_batch(
    spark: SparkSession,
    delta_table_path: str,
    api_endpoint: str,
    headers: Dict[str, str],
    max_calls_per_minute: int = 500,
    max_payload_size_mb: float = 1.0,
    num_partitions: int = 100,
    batch_size: int = 10000
) -> DataFrame:
    """
    Send Delta table to Adobe API using distributed batch processing

    Args:
        spark: SparkSession
        delta_table_path: Path to Delta table
        api_endpoint: Adobe API endpoint
        headers: HTTP headers with auth
        max_calls_per_minute: Rate limit
        max_payload_size_mb: Max payload size in MB
        num_partitions: Number of parallel partitions
        batch_size: Number of rows to process per batch (for large tables)

    Returns:
        DataFrame with results
    """
    max_payload_size_bytes = int(max_payload_size_mb * 1024 * 1024)

    print(f"{'='*80}")
    print(f"🚀 Adobe API Batch Sender - Starting")
    print(f"{'='*80}")
    print(f"Source: {delta_table_path}")
    print(f"Endpoint: {api_endpoint}")
    print(f"Rate Limit: {max_calls_per_minute} calls/min")
    print(f"Max Payload: {max_payload_size_mb} MB")
    print(f"Partitions: {num_partitions}")

    # Read Delta table
    df = spark.read.format("delta").load(delta_table_path)
    total_rows = df.count()
    print(f"Total Rows: {total_rows:,}")

    # Prepare DataFrame
    df_prepared = prepare_dataframe_for_api(df, num_partitions)

    # Define result schema
    result_schema = StructType([
        StructField("payload_id", StringType(), True),
        StructField("partition_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("error", StringType(), True),
        StructField("rows_sent", IntegerType(), True),
        StructField("payload_size_bytes", LongType(), True),
        StructField("timestamp", StringType(), True),
        StructField("attempts", IntegerType(), True)
    ])

    start_time = time.time()

    # Use mapInPandas for distributed processing
    results_df = df_prepared.mapInPandas(
        lambda iterator: send_to_adobe_api(
            iterator=iterator,
            api_endpoint=api_endpoint,
            headers=headers,
            max_calls_per_minute=max_calls_per_minute,
            max_payload_size_bytes=max_payload_size_bytes,
            retry_attempts=3
        ),
        schema=result_schema
    )

    # Materialize results
    results_df.cache()
    result_count = results_df.count()

    elapsed_time = time.time() - start_time

    # Calculate statistics
    success_count = results_df.filter(col("status") == "success").count()
    error_count = results_df.filter(col("status") == "error").count()
    total_rows_sent = results_df.agg({"rows_sent": "sum"}).collect()[0][0]
    avg_payload_size = results_df.agg({"payload_size_bytes": "avg"}).collect()[0][0]

    print(f"\n{'='*80}")
    print(f"✅ Processing Complete!")
    print(f"{'='*80}")
    print(f"Duration: {elapsed_time:.2f} seconds")
    print(f"Average Rate: {result_count/elapsed_time:.1f} requests/second")
    print(f"Total API Calls: {result_count}")
    print(f"Successful: {success_count} ({success_count/result_count*100:.1f}%)")
    print(f"Errors: {error_count}")
    print(f"Total Rows Sent: {total_rows_sent:,}")
    print(f"Avg Payload Size: {avg_payload_size/1024:.2f} KB")
    print(f"{'='*80}\n")

    return results_df


def send_delta_to_adobe_api_streaming(
    spark: SparkSession,
    delta_table_path: str,
    api_endpoint: str,
    headers: Dict[str, str],
    checkpoint_path: str,
    max_calls_per_minute: int = 500,
    max_payload_size_mb: float = 1.0,
    num_partitions: int = 100,
    trigger_interval: str = "30 seconds"
):
    """
    Send Delta table to Adobe API using Structured Streaming

    Args:
        spark: SparkSession
        delta_table_path: Path to Delta table
        api_endpoint: Adobe API endpoint
        headers: HTTP headers
        checkpoint_path: Path for streaming checkpoint
        max_calls_per_minute: Rate limit
        max_payload_size_mb: Max payload size
        num_partitions: Number of partitions
        trigger_interval: Trigger processing interval

    Returns:
        Streaming query object
    """
    max_payload_size_bytes = int(max_payload_size_mb * 1024 * 1024)

    print(f"🌊 Starting Structured Streaming to Adobe API...")

    # Read as stream
    streaming_df = (
        spark.readStream
        .format("delta")
        .load(delta_table_path)
    )

    # Prepare for API
    streaming_df_prepared = (
        streaming_df
        .withColumn("row_id", monotonically_increasing_id())
        .repartition(num_partitions)
        .withColumn("partition_id", spark_partition_id())
        .withColumn("num_partitions", lit(num_partitions))
    )

    # Result schema
    result_schema = StructType([
        StructField("payload_id", StringType(), True),
        StructField("partition_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("error", StringType(), True),
        StructField("rows_sent", IntegerType(), True),
        StructField("payload_size_bytes", LongType(), True),
        StructField("timestamp", StringType(), True),
        StructField("attempts", IntegerType(), True)
    ])

    # Apply transformation
    results_stream = streaming_df_prepared.mapInPandas(
        lambda iterator: send_to_adobe_api(
            iterator=iterator,
            api_endpoint=api_endpoint,
            headers=headers,
            max_calls_per_minute=max_calls_per_minute,
            max_payload_size_bytes=max_payload_size_bytes,
            retry_attempts=3
        ),
        schema=result_schema
    )

    # Write results to Delta for monitoring
    query = (
        results_stream
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start(f"{checkpoint_path}/results")
    )

    print(f"✅ Streaming query started: {query.id}")
    return query


# =============================================================================
# DATABRICKS NOTEBOOK CELLS
# =============================================================================

"""
# ============================================================================
# Cell 1: Configuration
# ============================================================================

# Adobe API Configuration
ADOBE_API_ENDPOINT = "https://dcs.adobedc.net/collection/your-endpoint"
ADOBE_ORG_ID = "your-org-id@AdobeOrg"
ADOBE_API_KEY = "your-api-key"
ADOBE_ACCESS_TOKEN = "your-access-token"

# Construct headers based on Adobe API requirements
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "x-api-key": ADOBE_API_KEY,
    "x-gw-ims-org-id": ADOBE_ORG_ID,
    "Authorization": f"Bearer {ADOBE_ACCESS_TOKEN}"
}

# Paths
DELTA_TABLE_PATH = "dbfs:/mnt/delta/your_table"
# OR use table name
# DELTA_TABLE_NAME = "catalog.schema.your_table"

CHECKPOINT_PATH = "dbfs:/mnt/checkpoints/adobe_api/"
RESULTS_PATH = "dbfs:/mnt/results/adobe_api_results/"

# Performance Settings
NUM_PARTITIONS = 100  # Number of parallel workers
MAX_CALLS_PER_MINUTE = 500
MAX_PAYLOAD_SIZE_MB = 1.0

print("✅ Configuration loaded")


# ============================================================================
# Cell 2: Load Module
# ============================================================================

from adobe_api_rest_sink import (
    send_delta_to_adobe_api_batch,
    send_delta_to_adobe_api_streaming
)

print("✅ Module imported")


# ============================================================================
# Cell 3: BATCH Processing - Process Entire Delta Table Once
# ============================================================================

# This is the FASTEST way to process an existing Delta table
results_df = send_delta_to_adobe_api_batch(
    spark=spark,
    delta_table_path=DELTA_TABLE_PATH,
    api_endpoint=ADOBE_API_ENDPOINT,
    headers=headers,
    max_calls_per_minute=MAX_CALLS_PER_MINUTE,
    max_payload_size_mb=MAX_PAYLOAD_SIZE_MB,
    num_partitions=NUM_PARTITIONS
)

# Save results
results_df.write.format("delta").mode("overwrite").save(RESULTS_PATH)

# Display results
display(results_df)


# ============================================================================
# Cell 4: Check Results Summary
# ============================================================================

# Read results
results_df = spark.read.format("delta").load(RESULTS_PATH)

# Summary statistics
print("📊 API Call Results Summary:")
print("="*60)

summary = results_df.groupBy("status").count().orderBy("status")
display(summary)

# Error details
errors_df = results_df.filter(col("status") == "error")
if errors_df.count() > 0:
    print(f"\n⚠️  Found {errors_df.count()} errors:")
    display(errors_df.select("payload_id", "partition_id", "error", "timestamp"))

# Success rate by partition
partition_stats = (
    results_df
    .groupBy("partition_id")
    .agg(
        {"status": "count", "rows_sent": "sum", "payload_size_bytes": "avg"}
    )
    .orderBy("partition_id")
)
display(partition_stats)


# ============================================================================
# Cell 5: STREAMING - Continuous Processing (Alternative)
# ============================================================================

# Use this for continuous processing of new data
streaming_query = send_delta_to_adobe_api_streaming(
    spark=spark,
    delta_table_path=DELTA_TABLE_PATH,
    api_endpoint=ADOBE_API_ENDPOINT,
    headers=headers,
    checkpoint_path=CHECKPOINT_PATH,
    max_calls_per_minute=MAX_CALLS_PER_MINUTE,
    max_payload_size_mb=MAX_PAYLOAD_SIZE_MB,
    num_partitions=NUM_PARTITIONS,
    trigger_interval="30 seconds"
)

print(f"Query ID: {streaming_query.id}")


# ============================================================================
# Cell 6: Monitor Streaming (if using streaming)
# ============================================================================

# Check status
print(f"Active: {streaming_query.isActive}")
print(f"Status: {streaming_query.status}")

# Last progress
if streaming_query.lastProgress:
    progress = streaming_query.lastProgress
    print(f"Batch ID: {progress.get('batchId')}")
    print(f"Input rows: {progress.get('numInputRows')}")
    print(f"Processing time: {progress.get('durationMs', {}).get('triggerExecution')} ms")

# Read streaming results
streaming_results = spark.read.format("delta").load(f"{CHECKPOINT_PATH}/results")
display(streaming_results.groupBy("status").count())


# ============================================================================
# Cell 7: Stop Streaming (if using streaming)
# ============================================================================

streaming_query.stop()
print("✅ Streaming stopped")
"""


# =============================================================================
# ADVANCED: Process Large Tables in Chunks
# =============================================================================

def process_large_table_in_chunks(
    spark: SparkSession,
    delta_table_path: str,
    api_endpoint: str,
    headers: Dict[str, str],
    partition_column: str,
    chunk_size: int = 1000000
):
    """
    Process very large Delta tables in chunks
    Useful for tables with billions of rows

    Args:
        partition_column: Column to use for chunking (e.g., 'date', 'id')
        chunk_size: Number of rows per chunk
    """
    print(f"🔄 Processing large table in chunks...")

    df = spark.read.format("delta").load(delta_table_path)

    # Get distinct partition values
    partitions = df.select(partition_column).distinct().collect()

    print(f"Found {len(partitions)} partitions to process")

    all_results = []

    for i, partition in enumerate(partitions, 1):
        partition_value = partition[0]
        print(f"\n[{i}/{len(partitions)}] Processing {partition_column}={partition_value}")

        # Filter to partition
        partition_df = df.filter(col(partition_column) == partition_value)
        partition_count = partition_df.count()

        if partition_count == 0:
            continue

        print(f"   Rows in partition: {partition_count:,}")

        # Process this partition
        results = send_delta_to_adobe_api_batch(
            spark=spark,
            delta_table_path=delta_table_path,
            api_endpoint=api_endpoint,
            headers=headers,
            num_partitions=50  # Fewer partitions per chunk
        )

        # Save results for this partition
        results.write.format("delta").mode("append").save(
            f"dbfs:/results/adobe_api_by_partition/{partition_column}={partition_value}/"
        )

        all_results.append(results)

        print(f"   ✅ Partition complete")

    print("\n✅ All partitions processed!")


# =============================================================================
# PERFORMANCE OPTIMIZATION TIPS
# =============================================================================

"""
Performance Tuning Guide:

1. CLUSTER CONFIGURATION:
   - Driver: Standard_DS3_v2 or larger
   - Workers: Standard_DS3_v2, 10-20 workers recommended
   - Enable Autoscaling: 5-20 workers
   - Runtime: DBR 13.3 LTS or later

2. OPTIMAL SETTINGS:

   Small table (<1M rows):
   - NUM_PARTITIONS = 50
   - MAX_CALLS_PER_MINUTE = 500

   Medium table (1M-10M rows):
   - NUM_PARTITIONS = 100
   - MAX_CALLS_PER_MINUTE = 500

   Large table (>10M rows):
   - NUM_PARTITIONS = 200
   - MAX_CALLS_PER_MINUTE = 500
   - Use chunk-based processing

3. SPARK CONFIGURATION:
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
   spark.conf.set("spark.sql.shuffle.partitions", "200")

4. RATE LIMITING:
   - Each partition gets fair share: 500 calls/min ÷ NUM_PARTITIONS
   - With 100 partitions: each gets 5 calls/min = 1 call per 12 seconds
   - This prevents rate limit errors while maximizing throughput

5. MONITORING:
   - Check Spark UI "Stages" tab for task distribution
   - Monitor "SQL" tab for DataFrame operations
   - Use Ganglia metrics for cluster utilization

6. ESTIMATED THROUGHPUT:
   - With 500 calls/min limit and 100 rows/call:
     = 50,000 rows/minute
     = 3,000,000 rows/hour

   - For 10M rows: ~3-4 hours
   - For 100M rows: ~33 hours (consider chunking)

7. ERROR HANDLING:
   - Failed payloads are recorded in results DataFrame
   - Filter by status='error' to see failures
   - Reprocess failures by filtering source table

8. COST OPTIMIZATION:
   - Use Spot/Preemptible instances for workers
   - Set aggressive autoscaling min (e.g., 5 workers)
   - Use Delta Lake caching for repeated reads
"""


if __name__ == "__main__":
    print("Adobe API REST Sink - Databricks Optimized")
    print("\nFeatures:")
    print("✅ Distributed processing with Spark")
    print("✅ Partition-based rate limiting")
    print("✅ 1MB payload size management")
    print("✅ Automatic retry with exponential backoff")
    print("✅ Batch and streaming modes")
    print("✅ Comprehensive error tracking")
    print("\nReady for Databricks!")
