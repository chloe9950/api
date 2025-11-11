# Optimized Adobe Martech API Data Streaming Solution for Databricks
# Handles rate limiting (500 calls/min) and payload size limits (1MB)

import json
import time
import sys
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from collections import deque
from threading import Lock, Semaphore
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_json, struct, collect_list, size, length
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RateLimiter:
    """Token bucket rate limiter for API calls"""
    
    def __init__(self, calls_per_minute: int = 500, burst_size: int = 10):
        self.calls_per_minute = calls_per_minute
        self.burst_size = burst_size
        self.tokens = burst_size
        self.max_tokens = burst_size
        self.refill_rate = calls_per_minute / 60.0  # tokens per second
        self.last_refill = time.time()
        self.lock = Lock()
        
    def acquire(self, tokens: int = 1) -> float:
        """Acquire tokens, blocking if necessary. Returns wait time."""
        with self.lock:
            now = time.time()
            # Refill tokens based on elapsed time
            elapsed = now - self.last_refill
            self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
            self.last_refill = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0
            else:
                # Calculate wait time needed
                tokens_needed = tokens - self.tokens
                wait_time = tokens_needed / self.refill_rate
                return wait_time

class AdobeAPIClient:
    """Thread-safe Adobe Martech API client with connection pooling and retry logic"""
    
    def __init__(self, 
                 api_endpoint: str,
                 api_key: str,
                 org_id: str,
                 max_retries: int = 3,
                 timeout: int = 30,
                 pool_connections: int = 10,
                 pool_maxsize: int = 20):
        
        self.api_endpoint = api_endpoint
        self.api_key = api_key
        self.org_id = org_id
        self.timeout = timeout
        
        # Setup session with connection pooling and retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "GET"]
        )
        adapter = HTTPAdapter(
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
            max_retries=retry_strategy
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
            "x-gw-ims-org-id": self.org_id,
            "Accept": "application/json"
        })
    
    def send_batch(self, data: List[Dict]) -> Dict:
        """Send a batch of data to Adobe API"""
        try:
            response = self.session.post(
                self.api_endpoint,
                json={"data": data},
                timeout=self.timeout
            )
            response.raise_for_status()
            return {"success": True, "status_code": response.status_code, "response": response.json()}
        except requests.exceptions.RequestException as e:
            logger.error(f"API call failed: {str(e)}")
            return {"success": False, "error": str(e)}

class DataBatcher:
    """Intelligent batching to respect 1MB payload limit"""
    
    @staticmethod
    def calculate_payload_size(records: List[Dict]) -> int:
        """Calculate approximate payload size in bytes"""
        return len(json.dumps({"data": records}))
    
    @staticmethod
    def create_optimal_batches(records: List[Dict], max_size_mb: float = 0.95) -> List[List[Dict]]:
        """
        Create optimal batches that stay under the size limit.
        Using 0.95 MB as default to leave buffer for headers and encoding overhead.
        """
        max_size_bytes = int(max_size_mb * 1024 * 1024)
        batches = []
        current_batch = []
        current_size = len(json.dumps({"data": []}))  # Base payload size
        
        for record in records:
            record_size = len(json.dumps(record))
            
            # Check if adding this record would exceed the limit
            if current_size + record_size > max_size_bytes and current_batch:
                batches.append(current_batch)
                current_batch = [record]
                current_size = len(json.dumps({"data": [record]}))
            else:
                current_batch.append(record)
                current_size += record_size
        
        if current_batch:
            batches.append(current_batch)
        
        return batches

def process_micro_batch(batch_df: DataFrame, 
                       batch_id: int, 
                       api_client: AdobeAPIClient,
                       rate_limiter: RateLimiter,
                       max_parallel_calls: int = 8):
    """
    Process a micro-batch from Structured Streaming.
    This function is called by foreachBatch.
    """
    logger.info(f"Processing batch {batch_id}")
    
    # Convert DataFrame to JSON records
    # Collect as list of dictionaries
    records = batch_df.toJSON().collect()
    json_records = [json.loads(record) for record in records]
    
    if not json_records:
        logger.info(f"Batch {batch_id} is empty, skipping")
        return
    
    logger.info(f"Batch {batch_id} contains {len(json_records)} records")
    
    # Create optimal batches respecting 1MB limit
    batcher = DataBatcher()
    batches = batcher.create_optimal_batches(json_records)
    logger.info(f"Created {len(batches)} batches for API calls")
    
    # Process batches with parallel execution and rate limiting
    successful_calls = 0
    failed_calls = 0
    
    with ThreadPoolExecutor(max_workers=max_parallel_calls) as executor:
        futures = []
        
        for i, batch in enumerate(batches):
            # Acquire rate limit token
            wait_time = rate_limiter.acquire()
            if wait_time > 0:
                time.sleep(wait_time)
            
            # Submit API call to thread pool
            future = executor.submit(api_client.send_batch, batch)
            futures.append((i, future))
        
        # Collect results
        for batch_idx, future in futures:
            try:
                result = future.result(timeout=60)
                if result["success"]:
                    successful_calls += 1
                    logger.info(f"Batch {batch_id}-{batch_idx} sent successfully")
                else:
                    failed_calls += 1
                    logger.error(f"Batch {batch_id}-{batch_idx} failed: {result.get('error')}")
            except Exception as e:
                failed_calls += 1
                logger.error(f"Batch {batch_id}-{batch_idx} failed with exception: {str(e)}")
    
    logger.info(f"Batch {batch_id} completed. Success: {successful_calls}, Failed: {failed_calls}")
    
    # If there are failed calls, you might want to implement dead letter queue or retry logic here
    if failed_calls > 0:
        # Could write failed records to a Delta table for later retry
        pass

def calculate_optimal_parallelism(records_per_second: float,
                                 api_calls_per_minute: int = 500,
                                 avg_records_per_call: int = 50) -> Dict[str, int]:
    """
    Calculate optimal cluster size and parallelism settings.
    """
    calls_per_second_needed = records_per_second / avg_records_per_call
    api_calls_per_second_limit = api_calls_per_minute / 60.0
    
    # Add buffer for overhead (80% efficiency)
    efficiency_factor = 0.8
    effective_calls_per_worker = efficiency_factor
    
    # Calculate required workers
    workers_needed = max(1, int(calls_per_second_needed / (api_calls_per_second_limit * efficiency_factor)))
    
    # Calculate optimal parallelism (should not exceed API rate limit)
    optimal_parallelism = min(
        int(api_calls_per_second_limit),
        workers_needed * 2  # 2 threads per worker as baseline
    )
    
    return {
        "workers_needed": workers_needed,
        "optimal_parallelism": optimal_parallelism,
        "cores_per_worker": 8,  # Recommended for I/O bound workload
        "total_cores": workers_needed * 8
    }

# Main streaming job
def run_streaming_job(spark: SparkSession,
                     source_table_path: str,
                     adobe_config: Dict[str, str],
                     checkpoint_location: str,
                     max_records_per_batch: int = 10000):
    """
    Main function to run the streaming job from Delta table to Adobe API.
    """
    
    # Initialize API client and rate limiter
    api_client = AdobeAPIClient(
        api_endpoint=adobe_config["endpoint"],
        api_key=adobe_config["api_key"],
        org_id=adobe_config["org_id"]
    )
    
    # Rate limiter with 500 calls/min and burst of 10
    rate_limiter = RateLimiter(calls_per_minute=480, burst_size=10)  # Using 480 to leave buffer
    
    # Read from Delta table as stream
    stream_df = (spark
        .readStream
        .format("delta")
        .option("ignoreChanges", "true")  # Handle updates/deletes gracefully
        .option("startingVersion", "latest")  # Start from latest version
        .load(source_table_path)
    )
    
    # Transform nested structure to JSON if needed
    # Assuming your table already has the nested structure
    # If you need to create nested JSON from flat structure, add transformation here
    
    # Example transformation (adjust based on your schema):
    # transformed_df = stream_df.select(
    #     to_json(struct(col("*"))).alias("customer_data")
    # )
    
    # Calculate optimal parallelism
    expected_records_per_second = 1000  # Adjust based on your expected throughput
    parallelism_config = calculate_optimal_parallelism(expected_records_per_second)
    logger.info(f"Recommended cluster configuration: {parallelism_config}")
    
    # Define the streaming query with foreachBatch
    query = (stream_df
        .writeStream
        .foreachBatch(lambda df, batch_id: process_micro_batch(
            df, 
            batch_id, 
            api_client, 
            rate_limiter,
            max_parallel_calls=parallelism_config["optimal_parallelism"]
        ))
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="10 seconds")  # Adjust based on your latency requirements
        .outputMode("append")
        .start()
    )
    
    return query

# Alternative: Batch processing approach for non-streaming scenarios
def run_batch_processing(spark: SparkSession,
                        source_table_path: str,
                        adobe_config: Dict[str, str],
                        partition_columns: Optional[List[str]] = None):
    """
    Batch processing approach with optimal partitioning for maximum throughput.
    Use this if you don't need real-time streaming.
    """
    
    # Read Delta table
    df = spark.read.format("delta").load(source_table_path)
    
    # If you have partition columns (e.g., date), filter for specific partitions
    if partition_columns:
        # Example: process today's data
        from pyspark.sql.functions import current_date
        df = df.filter(col(partition_columns[0]) == current_date())
    
    # Calculate optimal number of partitions based on API rate limit
    total_records = df.count()
    avg_records_per_api_call = 50  # Adjust based on your data
    total_api_calls_needed = total_records / avg_records_per_api_call
    
    # With 500 calls/min limit, calculate time needed and partitions
    api_calls_per_minute = 480  # Leave buffer
    minutes_needed = total_api_calls_needed / api_calls_per_minute
    
    # Use 8 parallel workers as optimal for API calls
    optimal_partitions = min(
        8,  # Max parallel API callers
        int(total_records / 1000)  # At least 1000 records per partition
    )
    
    # Repartition for optimal parallelism
    df_repartitioned = df.repartition(optimal_partitions)
    
    # Initialize API client and rate limiter as broadcast variables
    api_config_broadcast = spark.sparkContext.broadcast(adobe_config)
    
    def send_partition_to_api(partition_iterator):
        """Process a partition of records"""
        # Initialize client for this partition
        api_client = AdobeAPIClient(
            api_endpoint=api_config_broadcast.value["endpoint"],
            api_key=api_config_broadcast.value["api_key"],
            org_id=api_config_broadcast.value["org_id"]
        )
        
        # Local rate limiter for this partition
        rate_limiter = RateLimiter(
            calls_per_minute=480 // optimal_partitions  # Distribute rate limit
        )
        
        # Collect records from partition
        records = []
        for row in partition_iterator:
            records.append(row.asDict())
        
        # Create batches and send
        batcher = DataBatcher()
        batches = batcher.create_optimal_batches(records)
        
        results = []
        for batch in batches:
            wait_time = rate_limiter.acquire()
            if wait_time > 0:
                time.sleep(wait_time)
            
            result = api_client.send_batch(batch)
            results.append(result)
        
        return results
    
    # Execute batch processing
    results_rdd = df_repartitioned.rdd.mapPartitions(send_partition_to_api)
    results = results_rdd.collect()
    
    # Log results
    successful = sum(1 for batch_results in results for r in batch_results if r["success"])
    failed = sum(1 for batch_results in results for r in batch_results if not r["success"])
    
    logger.info(f"Batch processing completed. Success: {successful}, Failed: {failed}")
    
    return results

# Entry point for Databricks notebook
if __name__ == "__main__":
    # Initialize Spark session (already available in Databricks)
    spark = SparkSession.builder.appName("AdobeAPIStreaming").getOrCreate()
    
    # Configuration
    adobe_config = {
        "endpoint": "https://api.adobe.io/your-endpoint",  # Replace with your endpoint
        "api_key": dbutils.secrets.get(scope="adobe", key="api_key"),  # Use Databricks secrets
        "org_id": dbutils.secrets.get(scope="adobe", key="org_id")
    }
    
    source_table_path = "/path/to/your/delta/table"  # Replace with your Delta table path
    checkpoint_location = "/tmp/adobe_streaming_checkpoint"
    
    # Choose approach based on your needs:
    
    # Option 1: Streaming approach for continuous processing
    query = run_streaming_job(
        spark=spark,
        source_table_path=source_table_path,
        adobe_config=adobe_config,
        checkpoint_location=checkpoint_location
    )
    
    # Keep the stream running
    # query.awaitTermination()
    
    # Option 2: Batch approach for one-time or scheduled processing
    # results = run_batch_processing(
    #     spark=spark,
    #     source_table_path=source_table_path,
    #     adobe_config=adobe_config
    # )
