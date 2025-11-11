"""
Optimized Adobe Marketing API Batch Sender for Databricks
Sends JSON files with rate limiting (500 calls/min) and payload size limits (1MB)
"""

import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore, Lock
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import os


class AdobeAPIBatchSender:
    """
    High-performance Adobe API client with intelligent rate limiting
    """

    def __init__(
        self,
        api_endpoint: str,
        headers: Dict[str, str],
        max_calls_per_minute: int = 500,
        max_payload_size_mb: float = 1.0,
        max_workers: int = 50,
        retry_attempts: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize the Adobe API batch sender

        Args:
            api_endpoint: Adobe API endpoint URL
            headers: HTTP headers including authentication
            max_calls_per_minute: Rate limit (default: 500)
            max_payload_size_mb: Max payload size in MB (default: 1.0)
            max_workers: Number of concurrent threads (default: 50)
            retry_attempts: Number of retry attempts for failed requests
            retry_delay: Initial delay between retries in seconds
        """
        self.api_endpoint = api_endpoint
        self.headers = headers
        self.max_calls_per_minute = max_calls_per_minute
        self.max_payload_size_bytes = int(max_payload_size_mb * 1024 * 1024)
        self.max_workers = max_workers
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay

        # Rate limiting infrastructure
        self.semaphore = Semaphore(max_workers)
        self.lock = Lock()
        self.call_timestamps = []

        # Results tracking
        self.success_count = 0
        self.error_count = 0
        self.errors = []
        self.results = []

    def _wait_for_rate_limit(self):
        """
        Intelligent rate limiting: only wait when necessary
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

    def _send_single_request(
        self,
        payload: Dict[str, Any],
        payload_id: str,
        attempt: int = 1
    ) -> Dict[str, Any]:
        """
        Send a single API request with retry logic
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

                # Check for rate limit errors (429) or server errors (5xx)
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt < self.retry_attempts:
                        retry_delay = self.retry_delay * (2 ** (attempt - 1))  # Exponential backoff
                        time.sleep(retry_delay)
                        return self._send_single_request(payload, payload_id, attempt + 1)

                response.raise_for_status()

                with self.lock:
                    self.success_count += 1

                return {
                    "payload_id": payload_id,
                    "status": "success",
                    "status_code": response.status_code,
                    "response": response.json() if response.content else None,
                    "timestamp": datetime.now().isoformat()
                }

            except requests.exceptions.RequestException as e:
                # Retry on network errors
                if attempt < self.retry_attempts:
                    retry_delay = self.retry_delay * (2 ** (attempt - 1))
                    time.sleep(retry_delay)
                    return self._send_single_request(payload, payload_id, attempt + 1)

                # Record error after all retries exhausted
                with self.lock:
                    self.error_count += 1
                    error_info = {
                        "payload_id": payload_id,
                        "error": str(e),
                        "status_code": getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                        "timestamp": datetime.now().isoformat()
                    }
                    self.errors.append(error_info)

                return {
                    "payload_id": payload_id,
                    "status": "error",
                    "error": str(e),
                    "attempts": attempt
                }

    def send_batch(self, payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Send multiple payloads in parallel with rate limiting

        Args:
            payloads: List of dictionaries, each with 'data' and optional 'id'

        Returns:
            List of result dictionaries
        """
        print(f"🚀 Starting batch send of {len(payloads)} requests...")
        print(f"   Max concurrent workers: {self.max_workers}")
        print(f"   Rate limit: {self.max_calls_per_minute} calls/minute")
        print(f"   Max payload size: {self.max_payload_size_bytes / 1024 / 1024:.2f} MB")

        start_time = time.time()
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_payload = {
                executor.submit(
                    self._send_single_request,
                    payload.get('data', payload),
                    payload.get('id', f"payload_{idx}")
                ): idx
                for idx, payload in enumerate(payloads)
            }

            # Process completed tasks
            for i, future in enumerate(as_completed(future_to_payload), 1):
                result = future.result()
                results.append(result)

                # Progress update
                if i % 50 == 0 or i == len(payloads):
                    elapsed = time.time() - start_time
                    rate = i / elapsed if elapsed > 0 else 0
                    eta = (len(payloads) - i) / rate if rate > 0 else 0
                    print(f"   Progress: {i}/{len(payloads)} ({i*100//len(payloads)}%) | "
                          f"Rate: {rate:.1f} req/s | Success: {self.success_count} | "
                          f"Errors: {self.error_count} | ETA: {eta:.0f}s")

        elapsed_time = time.time() - start_time

        print(f"\n✅ Batch complete!")
        print(f"   Total time: {elapsed_time:.2f} seconds")
        print(f"   Average rate: {len(payloads)/elapsed_time:.1f} requests/second")
        print(f"   Success: {self.success_count} | Errors: {self.error_count}")

        if self.error_count > 0:
            print(f"\n⚠️  {self.error_count} requests failed. Check error details.")

        self.results = results
        return results

    def get_error_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all errors"""
        return self.errors

    def get_success_rate(self) -> float:
        """Calculate success rate percentage"""
        total = self.success_count + self.error_count
        return (self.success_count / total * 100) if total > 0 else 0


def load_json_files_from_dbfs(
    dbfs_path: str,
    file_pattern: str = "*.json"
) -> List[Dict[str, Any]]:
    """
    Load JSON files from DBFS path

    Args:
        dbfs_path: Path to DBFS directory containing JSON files
        file_pattern: Glob pattern for JSON files (default: *.json)

    Returns:
        List of dictionaries with 'id' and 'data' keys
    """
    print(f"📁 Loading JSON files from: {dbfs_path}")

    # Convert DBFS path to local path
    local_path = dbfs_path.replace("dbfs:/", "/dbfs/")

    import glob
    json_files = glob.glob(os.path.join(local_path, file_pattern))

    print(f"   Found {len(json_files)} JSON files")

    payloads = []
    for file_path in json_files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                payloads.append({
                    'id': os.path.basename(file_path),
                    'data': data
                })
        except Exception as e:
            print(f"   ⚠️  Error loading {file_path}: {e}")

    print(f"   Successfully loaded {len(payloads)} JSON files")
    return payloads


def validate_payload_sizes(
    payloads: List[Dict[str, Any]],
    max_size_bytes: int
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Validate payload sizes and separate oversized payloads

    Returns:
        Tuple of (valid_payloads, oversized_payloads)
    """
    valid = []
    oversized = []

    for payload in payloads:
        size = len(json.dumps(payload.get('data', payload)).encode('utf-8'))
        if size <= max_size_bytes:
            valid.append(payload)
        else:
            oversized.append({
                'id': payload.get('id', 'unknown'),
                'size_mb': size / 1024 / 1024
            })

    if oversized:
        print(f"\n⚠️  Warning: {len(oversized)} payloads exceed size limit:")
        for item in oversized[:5]:  # Show first 5
            print(f"   - {item['id']}: {item['size_mb']:.2f} MB")

    return valid, oversized


# =============================================================================
# DATABRICKS NOTEBOOK USAGE
# =============================================================================

"""
# Cell 1: Configuration
# =====================

ADOBE_API_ENDPOINT = "https://api.adobe.io/your-endpoint"
ADOBE_ORG_ID = "your-org-id@AdobeOrg"
ADOBE_API_KEY = "your-api-key"
ADOBE_ACCESS_TOKEN = "your-access-token"

# Headers for Adobe API
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "x-api-key": ADOBE_API_KEY,
    "x-gw-ims-org-id": ADOBE_ORG_ID,
    "Authorization": f"Bearer {ADOBE_ACCESS_TOKEN}"
}

# Path to your JSON files in DBFS
JSON_FILES_PATH = "dbfs:/mnt/data/json_files/"

# Performance settings
MAX_WORKERS = 50  # Adjust based on cluster size
MAX_CALLS_PER_MINUTE = 500
MAX_PAYLOAD_SIZE_MB = 1.0


# Cell 2: Load JSON Files
# ========================

payloads = load_json_files_from_dbfs(
    dbfs_path=JSON_FILES_PATH,
    file_pattern="*.json"
)

print(f"Total payloads loaded: {len(payloads)}")


# Cell 3: Validate Payload Sizes
# ===============================

valid_payloads, oversized_payloads = validate_payload_sizes(
    payloads=payloads,
    max_size_bytes=int(MAX_PAYLOAD_SIZE_MB * 1024 * 1024)
)

print(f"Valid payloads: {len(valid_payloads)}")
print(f"Oversized payloads: {len(oversized_payloads)}")


# Cell 4: Send to Adobe API
# ==========================

sender = AdobeAPIBatchSender(
    api_endpoint=ADOBE_API_ENDPOINT,
    headers=headers,
    max_calls_per_minute=MAX_CALLS_PER_MINUTE,
    max_payload_size_mb=MAX_PAYLOAD_SIZE_MB,
    max_workers=MAX_WORKERS,
    retry_attempts=3,
    retry_delay=1.0
)

results = sender.send_batch(valid_payloads)

print(f"\n📊 Final Results:")
print(f"   Success rate: {sender.get_success_rate():.2f}%")


# Cell 5: Save Results to Delta Table
# ====================================

from pyspark.sql import SparkSession

# Convert results to DataFrame
results_df = spark.createDataFrame(results)

# Save to Delta table
results_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("dbfs:/mnt/data/adobe_api_results/")

print("✅ Results saved to Delta table")

# Display results
display(results_df)


# Cell 6: Error Handling (Optional)
# ==================================

if sender.error_count > 0:
    errors_df = spark.createDataFrame(sender.get_error_summary())

    # Save errors for analysis
    errors_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("dbfs:/mnt/data/adobe_api_errors/")

    print(f"⚠️  Errors saved to Delta table")
    display(errors_df)
"""


# =============================================================================
# ALTERNATIVE: Direct payload list (not from files)
# =============================================================================

def send_payload_list(
    payloads: List[Dict[str, Any]],
    api_endpoint: str,
    headers: Dict[str, str],
    max_workers: int = 50
) -> List[Dict[str, Any]]:
    """
    Quick function to send a list of payloads directly

    Example:
        payloads = [
            {"user_id": "123", "event": "click"},
            {"user_id": "456", "event": "view"},
            ...
        ]

        results = send_payload_list(payloads, ADOBE_API_ENDPOINT, headers)
    """
    sender = AdobeAPIBatchSender(
        api_endpoint=api_endpoint,
        headers=headers,
        max_calls_per_minute=500,
        max_payload_size_mb=1.0,
        max_workers=max_workers
    )

    # Format payloads if needed
    formatted_payloads = [
        {'id': f'payload_{i}', 'data': p}
        for i, p in enumerate(payloads)
    ]

    return sender.send_batch(formatted_payloads)


if __name__ == "__main__":
    print("Adobe API Batch Sender - Ready to use in Databricks!")
