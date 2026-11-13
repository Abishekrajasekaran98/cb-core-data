from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel
from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    col, when, lit, transform, struct, concat_ws, size, to_json)
import requests
from requests.auth import HTTPBasicAuth
import json
from typing import Optional
from google.cloud import storage
import os
import zipfile
import shutil
from confluent_kafka import Producer
from kafka import KafkaProducer
import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
import multiprocessing
import posixpath
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

def druidDFOption(query: str, host: str, result_format: str = "object", limit: int = 10000,
                  spark: SparkSession = None) -> Optional[DataFrame]:
    """
    PySpark version of druidDFOption function
    """
    if spark is None:
        spark = SparkSession.getActiveSession()

    # Try using Spark Druid connector if available
    try:
        # Check if Druid connector is available
        df = spark.read \
            .format("druid") \
            .option("url", f"http://{host}:8888/druid/v2/sql") \
            .option("query", query) \
            .option("resultFormat", result_format) \
            .option("limit", limit) \
            .load()

        if df.count() == 0:
            print("ERROR: Druid connector returned empty dataframe")
            return None

        return df.persist(StorageLevel.MEMORY_ONLY)

    except Exception as e:
        print(f"Druid connector not available or failed")
        print("Falling back to HTTP API approach")

        # Fallback to HTTP API approach
        result = druidSQLAPI(query, host, result_format, limit).strip()

        # return None if result is an empty string
        if result == "":
            print("ERROR: druidSQLAPI returned empty string")
            return None

        df = dataframe_from_json_string(result, spark).persist(StorageLevel.MEMORY_ONLY)

        if df.count() == 0:
            print("ERROR: druidSQLAPI json parse result is empty")
            return None

        # return None if there is an `error` field in the json
        if has_column(df, "error"):
            print(f"ERROR: druidSQLAPI returned error response, response={result}")
            return None

        # now that error handling is done, proceed with business as usual
        return df


def druidSQLAPI(query: str, host: str, result_format: str = "object", limit: int = 10000) -> str:
    """
    PySpark version of druidSQLAPI function
    """
    # TODO: tech-debt, use proper spark druid connector when available
    url = f"http://{host}:8888/druid/v2/sql"
    request_body = {
        "resultFormat": result_format,
        "header": False,
        "context": {"sqlOuterLimit": limit},
        "query": query
    }

    return api("POST", url, json.dumps(request_body))


def api(method: str, url: str, body: str) -> str:
    """
    Simple HTTP API call function
    """
    try:
        if method.upper() == "POST":
            response = requests.post(
                url,
                data=body,
                headers={'Content-Type': 'application/json'}
            )
        elif method.upper() == "GET":
            response = requests.get(url)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()
        return response.text

    except requests.exceptions.RequestException as e:
        print(f"ERROR: API call failed: {e}")
        return ""


def sync_reports(local_path, remote_path, config):
    """
    Upload all files from `local_path` to GCS path: gs://<container>/<remote_path> using GCP service account.

    Parameters:
        local_path (str): Local directory to upload.
        remote_path (str): GCS path under the bucket (like 'reports/standalone-reports/merged').
        conf (object): Configuration object with attributes:
            - conf.container (str): GCS bucket name
            - conf.store (str): Expected to be 'gcs'
            - conf.gcp_service_account_key (str): Path to GCP credentials JSON
    """
    print(f"REPORT: Syncing reports from {local_path} to gs://{config.gcpBucket}/{remote_path} ...")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.googleServiceAccountFilePath
    client = storage.Client()
    bucket = client.bucket(config.gcpBucket)

    if os.path.isfile(local_path):   # single file case
        filename = os.path.basename(local_path)   # keep whatever CSV name was generated
        gcs_blob_path = os.path.join(remote_path, filename).replace("\\", "/")
        blob = bucket.blob(gcs_blob_path)
        blob.upload_from_filename(local_path)
        print(f"✅ Synced: {local_path} → gs://{config.gcpBucket}/{gcs_blob_path}")

    else:   # directory case (unchanged)
        for root, _, files in os.walk(local_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_path)
                gcs_blob_path = os.path.join(remote_path, relative_path).replace("\\", "/")
                blob = bucket.blob(gcs_blob_path)
                blob.upload_from_filename(local_file_path)
                print(f"✅ Synced: {local_file_path} → gs://{config.gcpBucket}/{gcs_blob_path}")

    print(f"REPORT: Finished syncing reports from {local_path} to gs://{config.gcpBucket}/{remote_path}")

def sync_reports_splat(local_path, remote_path, config):
    """
    Upload all files from `local_path` to gs://<splatGcpBucket>/<remote_path>
    using the splat HMAC access key + secret key.
    """
    print(f"REPORT: Syncing reports from {local_path} to gs://{config.splatGcpBucket}/{remote_path} ...")

    s3 = boto3.client(
        "s3",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=config.splatHmacAccessKey,
        aws_secret_access_key=config.splatHmacSecretKey,
        region_name="auto",
        # CHANGE 1: newer boto3 adds checksum headers that GCS rejects (SignatureDoesNotMatch)
        config=Config(signature_version="s3v4",
                      request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"),
    )

    # CHANGE 2: GCS rejects boto3's multipart "UploadPart" -> upload each file in one go
    # (GCS allows up to 5 TB in a single upload)
    single_put = TransferConfig(multipart_threshold=5 * 1024 ** 4, use_threads=False)

    if os.path.isfile(local_path):   # single file case
        filename = os.path.basename(local_path)
        gcs_blob_path = os.path.join(remote_path, filename).replace("\\", "/")
        s3.upload_file(local_path, config.splatGcpBucket, gcs_blob_path, Config=single_put)
        print(f"✅ Synced: {local_path} → gs://{config.splatGcpBucket}/{gcs_blob_path}")

    else:   # directory case
        for root, _, files in os.walk(local_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_path)
                gcs_blob_path = os.path.join(remote_path, relative_path).replace("\\", "/")
                s3.upload_file(local_file_path, config.splatGcpBucket, gcs_blob_path, Config=single_put)
                print(f"✅ Synced: {local_file_path} → gs://{config.splatGcpBucket}/{gcs_blob_path}")

    print(f"REPORT: Finished syncing reports from {local_path} to gs://{config.splatGcpBucket}/{remote_path}")

_GCS_CLIENTS = {}
_GCS_CLIENTS_LOCK = threading.Lock()


def _get_gcs_client(config, pool_size):
    """
    Return one cached storage.Client per credentials file.
    Building a client per call (as sync_reports does) repeats the auth set-up
    for every folder that is uploaded.
    """
    key = config.googleServiceAccountFilePath
    with _GCS_CLIENTS_LOCK:
        client = _GCS_CLIENTS.get(key)
        if client is None:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key
            client = storage.Client()
            # requests keeps only 10 pooled connections by default; with more
            # upload threads than that, extra connections are opened and thrown
            # away on every request. Widen the pool to match the thread count.
            try:
                from requests.adapters import HTTPAdapter
                adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
                client._http.mount("https://", adapter)
            except Exception as e:  # tuning only: never let it break an upload
                print(f"WARNING: could not widen the GCS connection pool ({e}); continuing with defaults")
            _GCS_CLIENTS[key] = client
        return client


def _collect_upload_files(local_path, remote_path):
    """Return [(local_file, blob_name)] using the same naming rule as sync_reports."""
    if os.path.isfile(local_path):
        return [(local_path, posixpath.join(remote_path, os.path.basename(local_path)))]

    files = []
    for root, _, names in os.walk(local_path):
        for name in names:
            local_file = os.path.join(root, name)
            relative = os.path.relpath(local_file, local_path).replace("\\", "/")
            files.append((local_file, posixpath.join(remote_path, relative)))
    return files


def sync_reports_parallel(local_path, remote_path, config, max_workers=32, retries=2,
                          chunk_size=2000, _uploader=None, _bucket=None):
    """
    Parallel replacement for sync_reports: upload a file, or every file under a
    directory, to gs://<config.gcpBucket>/<remote_path>.

    Same as sync_reports:
      * identical object names, in file and directory mode
      * raises if any file cannot be uploaded

    Different:
      * uploads run in `max_workers` threads (the work is network wait, so
        threads scale; PROCESS workers are avoided on purpose because they
        fork, which is unsafe in a process that hosts a Spark JVM)
      * one cached client per credentials file
      * each failed file is retried `retries` times, then ALL remaining
        failures are reported in one RuntimeError (not just the first)
      * a progress line per `chunk_size` files instead of one print per file

    Files are submitted in batches of `chunk_size`, and each batch waits for its
    slowest file, so one stalled upload can hold up a batch.

    `_uploader` and `_bucket` are test hooks; leave them unset in real code.
    """
    from google.cloud.storage import transfer_manager  # lazy: keeps older client versions importable

    print(f"REPORT: Syncing reports from {local_path} to gs://{config.gcpBucket}/{remote_path} "
          f"({max_workers} threads) ...")
    t0 = time.time()

    files = _collect_upload_files(local_path, remote_path)
    if not files:
        print(f"REPORT: nothing to upload under {local_path}")
        return 0

    bucket = _bucket if _bucket is not None else _get_gcs_client(config, max_workers).bucket(config.gcpBucket)
    uploader = _uploader or transfer_manager.upload_many

    pending = files
    for attempt in range(retries + 1):
        failed = []
        for start in range(0, len(pending), chunk_size):
            batch = pending[start:start + chunk_size]
            pairs = [(local_file, bucket.blob(blob_name)) for local_file, blob_name in batch]
            results = uploader(
                pairs,
                worker_type=transfer_manager.THREAD,
                max_workers=max_workers,
                raise_exception=False,
            )
            for item, result in zip(batch, results):
                if isinstance(result, Exception):
                    failed.append((item, result))
            if len(pending) > chunk_size:
                done = min(start + chunk_size, len(pending))
                print(f"   ... {done:,}/{len(pending):,} files attempted "
                      f"({len(failed)} failed so far, {time.time() - t0:.0f}s)")

        if not failed:
            break
        pending = [item for item, _ in failed]
        if attempt < retries:
            print(f"WARNING: {len(failed)} upload(s) failed, retrying ({attempt + 1}/{retries}) ...")
            time.sleep(2 * (attempt + 1))

    if failed:
        sample = "; ".join(f"{item[1]}: {type(err).__name__}: {err}" for item, err in failed[:5])
        raise RuntimeError(
            f"{len(failed)} of {len(files)} files failed to upload to "
            f"gs://{config.gcpBucket}/{remote_path} after {retries} retries. First failures: {sample}"
        )

    elapsed = time.time() - t0
    print(f"REPORT: Finished syncing {len(files):,} files from {local_path} to "
          f"gs://{config.gcpBucket}/{remote_path} in {elapsed:.1f}s ({len(files) / max(elapsed, 0.001):.0f} files/s)")
    return len(files)


def _zip_worker(args):
    """
    Runs inside a worker process. Returns (status, folder, n_files, seconds, message).
    Must stay a module-level function: spawned workers locate it by import path.
    """
    import pyminizip  # lazy: only zip jobs need it, and only the workers

    mdoid_folder, merged_dir, password, level = args
    t0 = time.time()
    mdoid_path = os.path.join(merged_dir, mdoid_folder)

    if not os.path.isdir(mdoid_path):
        return ("skip", mdoid_folder, 0, 0.0, "not a directory")
    csv_files = [f for f in os.listdir(mdoid_path) if f.endswith(".csv")]
    if not csv_files:
        return ("skip", mdoid_folder, 0, 0.0, "no csv files")

    zip_path = os.path.join(mdoid_path, "reports.zip")
    file_paths = [os.path.join(mdoid_path, f) for f in csv_files]
    try:
        pyminizip.compress_multiple(file_paths, [""] * len(file_paths), zip_path, str(password), level)
        # never delete the source CSVs unless the zip is really there
        if not os.path.isfile(zip_path) or os.path.getsize(zip_path) == 0:
            raise RuntimeError("zip is missing or empty after compress")
        for p in file_paths:
            os.remove(p)
        return ("ok", mdoid_folder, len(csv_files), time.time() - t0, "")
    except Exception as e:
        return ("fail", mdoid_folder, len(csv_files), time.time() - t0, f"{type(e).__name__}: {e}")


def _csv_bytes(task):
    """Total size of the .csv files in one (folder, merged_dir, password) task."""
    folder, merged_dir, _ = task
    total = 0
    try:
        with os.scandir(os.path.join(merged_dir, folder)) as it:
            for entry in it:
                if entry.name.endswith(".csv"):
                    total += entry.stat().st_size
    except OSError:
        pass
    return total


def zip_folders_parallel(tasks, workers=None, level=5, progress_every=2000,
                         big_folder_bytes=None, big_level=None, show_slowest=5):
    """
    Zip many folders in parallel: each folder's CSVs become <folder>/reports.zip
    (pyminizip, password protected) and the CSVs are removed.

    tasks: iterable of (mdoid_folder, merged_dir, password)
    workers: processes to use (default: CPU count, capped at 24)
    level: compression level (1-9)
    big_folder_bytes / big_level: optionally use a lower level for folders whose
        CSVs total at least big_folder_bytes (default: every folder uses `level`)

    Why processes: pyminizip holds the GIL for the whole call, so threads would
    run one zip at a time. Workers start with the 'spawn' method (fork is unsafe
    next to a live Spark JVM), so the calling script must keep its
    `if __name__ == "__main__":` guard: spawned workers import it again.

    Biggest folders are submitted first so one huge folder is not left running
    alone at the end, and completions are reported as they happen.

    Raises RuntimeError if any folder failed or any .csv was left behind, so a
    caller never goes on to upload plaintext reports. Returns a summary dict.
    """
    tasks = list(tasks)
    if not tasks:
        print("   nothing to zip")
        return {"zipped": 0, "skipped": 0, "failed": 0, "seconds": 0.0, "slowest_seconds": 0.0}

    if workers is None:
        workers = max(1, min(os.cpu_count() or 4, 24))

    t0 = time.time()
    sized = sorted(((task, _csv_bytes(task)) for task in tasks), key=lambda item: item[1], reverse=True)
    sizes = {task[0]: nbytes for task, nbytes in sized}
    work = []
    for (folder, merged_dir, password), nbytes in sized:
        use_level = big_level if (big_level is not None and big_folder_bytes is not None
                                  and nbytes >= big_folder_bytes) else level
        work.append((folder, merged_dir, password, use_level))
    n_low = sum(1 for w in work if w[3] != level)
    print(f"   zipping {len(work):,} folders with {workers} processes "
          f"(biggest first, sized in {time.time() - t0:.1f}s"
          f"{f', {n_low} giant folder(s) at level {big_level}' if n_low else ''})")

    zipped = skipped = done = 0
    failures = []
    timings = []
    t_zip = time.time()
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as executor:
        futures = [executor.submit(_zip_worker, w) for w in work]
        for future in as_completed(futures):
            status, folder, _n, secs, message = future.result()
            done += 1
            if status == "ok":
                zipped += 1
                timings.append((secs, folder))
            elif status == "skip":
                skipped += 1
            else:
                failures.append((folder, message))
            if progress_every and done % progress_every == 0:
                print(f"   ... {done:,}/{len(work):,} folders ({time.time() - t_zip:.1f}s, {len(failures)} failed)")

    leftovers = []
    for folder, merged_dir, _ in tasks:
        try:
            if any(name.endswith(".csv") for name in os.listdir(os.path.join(merged_dir, folder))):
                leftovers.append(folder)
        except OSError:
            pass

    elapsed = time.time() - t_zip
    print(f"   zipped {zipped:,} folders ({skipped} skipped, {len(failures)} failed) in {elapsed:.1f}s "
          f"with {workers} processes ({zipped / max(elapsed, 0.001):.1f} zips/s)")

    timings.sort(reverse=True)
    slowest = timings[0][0] if timings else 0.0
    if show_slowest and timings:
        print(f"   slowest {min(show_slowest, len(timings))} folders (critical path):")
        for secs, folder in timings[:show_slowest]:
            print(f"      {folder}: {secs:,.1f}s for {sizes.get(folder, 0) / 1e6:,.0f} MB of CSV")
        if workers > 1 and slowest > 0.7 * elapsed:
            print(f"   NOTE: the single slowest folder took {slowest:,.0f}s of the {elapsed:,.0f}s wall time: "
                  f"more workers cannot help; use big_folder_bytes/big_level for the giant folders")

    if failures or leftovers:
        sample = "; ".join(f"{f}: {m}" for f, m in failures[:5])
        raise RuntimeError(
            f"Zipping did not complete cleanly: {len(failures)} folder(s) failed, "
            f"{len(leftovers)} folder(s) still contain plaintext .csv files. "
            f"Do NOT sync merged_dir. First failures: {sample or 'n/a'}; "
            f"folders with leftover csv: {leftovers[:5]}"
        )

    return {"zipped": zipped, "skipped": skipped, "failed": 0, "seconds": elapsed, "slowest_seconds": slowest}


def zip_and_sync_reports(complete_path: str, report_path: str,config):
    """
    Zip report folder and sync to blob storage.
    Instance method version that can access self.sync_reports

    Args:
        complete_path: Complete local path to the report folder
        report_path: Remote path for blob storage sync
    """
    try:
        print(f"Starting zip and sync for: {complete_path}")

        folder = Path(complete_path)
        zip_file_path = f"{complete_path}.zip"

        # Step 1: Delete existing .zip file if it exists
        report_name = folder.name
        existing_zip_file = folder / f"{report_name}.zip"

        if existing_zip_file.exists():
            print(f"Deleting existing zip file: {existing_zip_file}")
            existing_zip_file.unlink()

        # Step 2: Delete .crc files (Hadoop checksum files)
        if folder.exists() and folder.is_dir():
            crc_files = list(folder.glob("*.crc"))
            if crc_files:
                print(f"Deleting {len(crc_files)} .crc files")
                for crc_file in crc_files:
                    crc_file.unlink()

        # Step 3: Zip the folder
        print(f"Creating zip file: {zip_file_path}")

        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zipf:
            for root, dirs, files in os.walk(complete_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, os.path.dirname(complete_path))
                    zipf.write(file_path, arcname)

        print(f"Zip file created successfully: {zip_file_path}")

        # Step 4: Clean directory contents
        if folder.exists() and folder.is_dir():
            print(f"Cleaning directory: {complete_path}")
            for item in folder.iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)

        # Step 5: Move zip file inside the parent directory
        zip_file_name = Path(zip_file_path).name
        destination_zip_file_path = folder / zip_file_name

        print(f"Moving zip file to: {destination_zip_file_path}")
        shutil.move(zip_file_path, str(destination_zip_file_path))

        # Step 6: Sync to blob storage
        print(f"Syncing to blob storage: {report_path}")
        sync_reports(complete_path, report_path,config)

        print(f"Successfully zipped and synced: {complete_path}")

    except Exception as e:
        print(f"Error in zip_and_sync_reports: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise

def dataframe_from_json_string(json_str: str, spark: SparkSession) -> DataFrame:
    """
    Convert JSON string to DataFrame
    """
    try:
        # Parse JSON string to get list of records
        data = json.loads(json_str)

        # Create DataFrame from JSON data
        if isinstance(data, list) and len(data) > 0:
            df = spark.read.json(spark.sparkContext.parallelize([json.dumps(record) for record in data]))
            return df
        else:
            # Return empty DataFrame with no schema
            return spark.createDataFrame([], schema=None)

    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse JSON: {e}")
        return spark.createDataFrame([], schema=None)
    except Exception as e:
        print(f"ERROR: Failed to create DataFrame from JSON: {e}")
        return spark.createDataFrame([], schema=None)


def read_cassandra_table(spark, keyspace: str, table: str) -> "DataFrame":
    return spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()


def has_column(df: DataFrame, column_name: str) -> bool:
    """
    Check if DataFrame has a specific column
    """
    return column_name in df.columns


def read_elasticsearch_data(spark: SparkSession, host: str, port: str, index: str, query: str, fields: list,
                            array_fields: list) -> "DataFrame":
    """Read data from Elasticsearch"""
    dfr = spark.read.format("org.elasticsearch.spark.sql") \
        .option("es.read.metadata", "false") \
        .option("es.nodes", host) \
        .option("es.port", port) \
        .option("es.index.auto.create", "false") \
        .option("es.nodes.wan.only", "true") \
        .option("es.nodes.discovery", "false")

    # Add array field handling if specified
    if array_fields:
        dfr = dfr.option("es.read.field.as.array.include", ",".join(array_fields))

    # Add query and load data
    df = dfr.option("query", query).load(index)

    # Select only the specified fields that actually exist
    if fields:
        # Filter to only existing fields
        existing_fields = [f for f in fields if f in df.columns]
        missing_fields = [f for f in fields if f not in df.columns]

        # Select existing fields
        df = df.select(*[col(f) for f in existing_fields])

        # Add missing fields as null
        for field in missing_fields:
            df = df.withColumn(field, lit(None).cast(StringType()))

    if 'responses' in df.columns:
        df = df.withColumnRenamed("responses", "responses_raw")
        df = df.withColumn(
            "responses",
            transform(
                col("responses_raw"),
                lambda x: struct(
                    x.question.alias("question"),
                    when(
                        x.answer.isNull() | (size(x.answer) == 0),
                        lit("NA")
                    ).otherwise(concat_ws(",", x.answer)).alias("answer"),
                    x.questionId.alias("questionId"),
                    x.answerType.alias("answerType")
                )
            )
        )
    # Persist with MEMORY_ONLY storage level for performance
    df = df.persist(StorageLevel.MEMORY_ONLY)

    # Force evaluation to ensure data is loaded
    count = df.count()
    print(f"Successfully loaded {count} rows from Elasticsearch index: {index}")

    return df

def read_elasticsearch_data_scroll(spark: SparkSession, host: str, port: str, index: str, fields: list = None, scroll_size: int = 1000, scroll_timeout: str = "2m", query: dict = None, es_user: str = None, es_pass: str = None) -> DataFrame:
    """
    Fetch all data from Elasticsearch index using scroll API.

    Parameters:
        spark (SparkSession): The Spark session.
        host (str): Elasticsearch host.
        port (str): Elasticsearch port.
        index (str): Index name.
        fields (list, optional): List of fields to fetch.
        scroll_size (int, optional): Number of docs per scroll batch. Default 1000.
        scroll_timeout (str, optional): Scroll context timeout (e.g. "2m"). Default "2m".
        query (dict, optional): Query DSL dict. Default match_all.
        es_user (str, optional): Elasticsearch username.
        es_pass (str, optional): Elasticsearch password.

    Returns:
        DataFrame: All documents from the index as a Spark DataFrame.
    """
    import requests
    from requests.auth import HTTPBasicAuth
    import json
    all_docs = []
    url = f"http://{host}:{port}/{index}/_search?scroll={scroll_timeout}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "size": scroll_size,
        "query": query if query else {"match_all": {}},
    }
    if fields:
        payload["_source"] = fields
    auth = HTTPBasicAuth(es_user, es_pass) if es_user and es_pass else None
    # Initial search
    resp = requests.post(url, headers=headers, data=json.dumps(payload), auth=auth)
    resp.raise_for_status()
    data = resp.json()
    scroll_id = data.get("_scroll_id")
    hits = data.get("hits", {}).get("hits", [])
    all_docs.extend([hit["_source"] for hit in hits])
    # Scroll loop
    while scroll_id and hits:
        scroll_url = f"http://{host}:{port}/_search/scroll"
        scroll_payload = {"scroll": scroll_timeout, "scroll_id": scroll_id}
        resp = requests.post(scroll_url, headers=headers, data=json.dumps(scroll_payload), auth=auth)
        resp.raise_for_status()
        data = resp.json()
        scroll_id = data.get("_scroll_id")
        hits = data.get("hits", {}).get("hits", [])
        all_docs.extend([hit["_source"] for hit in hits])
        if not hits:
            break
    # Convert to DataFrame
    if not all_docs:
        print(f"No documents found in index: {index}")
        return spark.createDataFrame([], schema=None)
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(doc) for doc in all_docs]))
    print(f"Loaded {df.count()} documents from ES index '{index}' using scroll API.")
    return df

def writeToCassandra(df, keyspace: str, table: str, mode: str = "append"):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .mode(mode) \
        .save()

def dispatch_df_to_kafka(df, topic: str, broker_list: str):
    if not topic:
        print("ERROR: topic is blank, skipping kafka dispatch")
        return
    if not broker_list:
        print("ERROR: broker list is blank, skipping kafka dispatch")
        return

    def send_partition(rows_iter):
        producer = KafkaProducer(
            bootstrap_servers=broker_list.split(","),
            value_serializer=lambda v: v.encode("utf-8"),
        )
        try:
            for row in rows_iter:
                # adjust serialization as needed
                value = json.dumps(row.asDict())
                producer.send(topic, value)
            producer.flush()
        finally:
            producer.close()

    df.foreachPartition(send_partition)

def dispatch_df_to_kafka_updated(df, topic: str, broker_list: str, num_partitions: int = 4):
    if not topic:
        print("ERROR: topic is blank, skipping kafka dispatch")
        return
    if not broker_list:
        print("ERROR: broker list is blank, skipping kafka dispatch")
        return

    payload_df = df.select(
        to_json(struct(*df.columns), {"ignoreNullFields": "false"}).alias("kafka_value")
    )

    current_partitions = payload_df.rdd.getNumPartitions()
    if current_partitions > num_partitions:
        print(f"🔧 Coalescing Kafka dispatch from {current_partitions} to {num_partitions} partitions")
        payload_df = payload_df.coalesce(num_partitions)

    def send_partition(rows_iter):
        from confluent_kafka import Producer  # import INSIDE the closure — required, see note below

        conf = {
            'bootstrap.servers': broker_list,
            'compression.type': 'lz4',
            'linger.ms': 100,
            'batch.size': 1048576,
            'queue.buffering.max.messages': 500000,
            'queue.buffering.max.kbytes': 524288,
            'acks': 1,
        }
        producer = Producer(conf)
        sent = 0

        def delivery_report(err, msg):
            if err is not None:
                print(f"❌ Delivery failed: {err}")

        for row in rows_iter:
            producer.produce(topic, value=row.kafka_value.encode("utf-8"), callback=delivery_report)
            sent += 1
            if sent % 50000 == 0:
                producer.poll(0)  # serve delivery callbacks periodically, non-blocking

        producer.flush()
        print(f"✅ Partition dispatched {sent} messages")

    payload_df.foreachPartition(send_partition)

def read_elasticsearch_data_scroll(spark: SparkSession, host: str, port: str, index: str, fields: list = None, scroll_size: int = 1000, scroll_timeout: str = "2m", query: dict = None, es_user: str = None, es_pass: str = None) -> DataFrame:
    """
    Fetch all data from Elasticsearch index using scroll API.

    Parameters:
        spark (SparkSession): The Spark session.
        host (str): Elasticsearch host.
        port (str): Elasticsearch port.
        index (str): Index name.
        fields (list, optional): List of fields to fetch.
        scroll_size (int, optional): Number of docs per scroll batch. Default 1000.
        scroll_timeout (str, optional): Scroll context timeout (e.g. "2m"). Default "2m".
        query (dict, optional): Query DSL dict. Default match_all.
        es_user (str, optional): Elasticsearch username.
        es_pass (str, optional): Elasticsearch password.

    Returns:
        DataFrame: All documents from the index as a Spark DataFrame.
    """

    all_docs = []
    url = f"http://{host}:{port}/{index}/_search?scroll={scroll_timeout}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "size": scroll_size,
        "query": query if query else {"match_all": {}},
    }
    if fields:
        payload["_source"] = fields
    auth = HTTPBasicAuth(es_user, es_pass) if es_user and es_pass else None
    # Initial search
    resp = requests.post(url, headers=headers, data=json.dumps(payload), auth=auth)
    resp.raise_for_status()
    data = resp.json()
    scroll_id = data.get("_scroll_id")
    hits = data.get("hits", {}).get("hits", [])
    all_docs.extend([hit["_source"] for hit in hits])
    # Scroll loop
    while scroll_id and hits:
        scroll_url = f"http://{host}:{port}/_search/scroll"
        scroll_payload = {"scroll": scroll_timeout, "scroll_id": scroll_id}
        resp = requests.post(scroll_url, headers=headers, data=json.dumps(scroll_payload), auth=auth)
        resp.raise_for_status()
        data = resp.json()
        scroll_id = data.get("_scroll_id")
        hits = data.get("hits", {}).get("hits", [])
        all_docs.extend([hit["_source"] for hit in hits])
        if not hits:
            break
    # Convert to DataFrame
    if not all_docs:
        print(f"No documents found in index: {index}")
        return spark.createDataFrame([], schema=None)
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(doc) for doc in all_docs]))
    print(f"Loaded {df.count()} documents from ES index '{index}' using scroll API.")
    return df