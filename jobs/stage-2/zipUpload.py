import findspark

findspark.init()

import time
import secrets
import string
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from zipfile import ZipFile, ZIP_STORED, ZIP_DEFLATED
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import shutil
import subprocess
import sys
import glob
import os
import pandas as pd
import pyzipper
import pyminizip
import polars as pl

sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.dfexport import dfexportutil
from dfutil.utils.utils import sync_reports_parallel, sync_reports_splat, zip_folders_parallel
from jobs.config import get_environment_config
from jobs.default_config import create_config
from dfutil.utils.redis import Redis


class ZipUploadModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.ZipUpload"

    def name(self):
        return "ZipUploadModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def generate_alphanumeric_password(length):
        """
        Generates a cryptographically secure alphanumeric password.
        Uses Python's secrets module — equivalent to Java's SecureRandom.
        """
        length = int(length)
        ALPHANUMERIC_CHARACTERS = string.ascii_letters + string.digits
        return ''.join(secrets.choice(ALPHANUMERIC_CHARACTERS) for _ in range(length))

    @staticmethod
    def generate_mdoid_passwords(spark, config, mdoid_folders, org_hierarchy_df):
        """
        Generate passwords grouped by effective L0 organisation.

        Priority:
          1. ministry_id present      → group by ministry_id (true L0)
          2. ministry_id null but
             department_id present    → treat department_id as L0 (fallback)
             All mdo_ids under that
             orphan dept share its
             password, including the
             dept folder itself.
          3. Both null               → individual password (true orphan)

        Returns:
            - mdoid_password_map : dict  {mdoid_folder: password}
            - password_df        : Spark DataFrame [mdo_id, password]
        """
        from pyspark.sql import functions as F

        # ── Step 1: one password per unique non-null ministry_id (true L0) ─
        ministry_ids = [
            row["ministry_id"]
            for row in org_hierarchy_df
                .select("ministry_id")
                .filter(F.col("ministry_id").isNotNull() & (F.col("ministry_id") != ""))
                .distinct()
                .collect()
        ]
        ministry_password_map = {
            mid: ZipUploadModel.generate_alphanumeric_password(config.password_length)
            for mid in ministry_ids
        }
        print(f"  Generated {len(ministry_password_map)} unique L0 (ministry) passwords")

        # ── Step 2: one password per orphan department_id (fallback L0) ───
        # Orphan dept = has no ministry_id but has a department_id
        orphan_dept_ids = [
            row["department_id"]
            for row in org_hierarchy_df
                .select("department_id")
                .filter(
                    (F.col("ministry_id").isNull() | (F.col("ministry_id") == "")) &
                    F.col("department_id").isNotNull() &
                    (F.col("department_id") != "")
                )
                .distinct()
                .collect()
        ]
        dept_password_map = {
            did: ZipUploadModel.generate_alphanumeric_password(config.password_length)
            for did in orphan_dept_ids
        }
        print(f"  Generated {len(dept_password_map)} unique orphan-dept (fallback L0) passwords")

        # ── Step 3: map every mdo_id → effective L0 password ──────────────
        mdo_rows = org_hierarchy_df \
            .select("mdo_id", "ministry_id", "department_id") \
            .filter(F.col("mdo_id").isNotNull()) \
            .distinct() \
            .collect()

        hierarchy_mdo_password = {}
        for row in mdo_rows:
            mdo = row["mdo_id"]
            mid = row["ministry_id"]
            did = row["department_id"]

            if mid and mid in ministry_password_map:
                # has ministry → true L0 password
                hierarchy_mdo_password[mdo] = ministry_password_map[mid]
            elif did and did in dept_password_map:
                # no ministry, has orphan dept → dept acts as L0
                hierarchy_mdo_password[mdo] = dept_password_map[did]
            # else: both null → individual password assigned in step 4

        # ── Step 4: assign password to every filesystem mdoid folder ──────
        mdoid_password_map = {}
        for folder in mdoid_folders:
            raw_id = folder.replace("mdoid=", "")

            if raw_id in hierarchy_mdo_password:
                # mapped in hierarchy → inherited password
                mdoid_password_map[folder] = hierarchy_mdo_password[raw_id]
            elif raw_id in ministry_password_map:
                # folder itself IS a ministry (L0) → its own password
                mdoid_password_map[folder] = ministry_password_map[raw_id]
            elif raw_id in dept_password_map:
                # folder itself IS an orphan dept (fallback L0) → its own password
                mdoid_password_map[folder] = dept_password_map[raw_id]
            else:
                # true orphan — no ministry, no dept → individual password
                mdoid_password_map[folder] = ZipUploadModel.generate_alphanumeric_password(
                    config.password_length
                )

        # ── Step 5: Spark DataFrame for audit / Redis write ────────────────
        schema = StructType([
            StructField("mdo_id",   StringType(), False),
            StructField("password", StringType(), False),
        ])
        rows = [Row(mdo_id=folder, password=pwd) for folder, pwd in mdoid_password_map.items()]
        password_df = spark.createDataFrame(rows, schema=schema)

        return mdoid_password_map, password_df
    
    def run_full_report(self, config):
        """Part 2: zip the selected warehouse parquet folders with 7z and upload the zip. Returns the seconds it took."""
        part2_start          = time.time()
        warehouse_base       = config.warehouseReportDir
        warehouse_output_dir = config.warehouseOutputDir
        warehouse_zip_path   = os.path.join(warehouse_output_dir, "reports.zip")

        # only these warehouse folders go into the zip
        SELECTED_FOLDERS = ["user_detail", "user_enrolments", "content", "content_resource",
            "cb_plan", "org_hierarchy_new", "assessment_detail", "bp_enrolments", "event_details",
            "event_enrolment_details", "kcm_content_mapping", "kcm_dictionary"]

        if os.path.exists(warehouse_output_dir):
            shutil.rmtree(warehouse_output_dir)
        os.makedirs(warehouse_output_dir)

        available = {f for f in os.listdir(warehouse_base)
                     if os.path.isdir(os.path.join(warehouse_base, f))}
        folders = [f for f in SELECTED_FOLDERS if f in available]
        missing = [f for f in SELECTED_FOLDERS if f not in available]
        if missing:
            print(f"⚠️  [Part 2] Not found in {warehouse_base}, skipping: {missing}")

        if folders:
            print(f"\n🗜️  [Part 2] Zipping {len(folders)} parquet folders: {folders}")
            zip_start = time.time()
            try:
                subprocess.run(
                    ["7z", "a", "-tzip", "-mx=0", "-mmt=on",
                     f"-p{config.password}", "-mem=ZipCrypto",
                     warehouse_zip_path, *folders,
                     "-xr!_SUCCESS", "-xr!*.crc"],
                    cwd=warehouse_base, check=True, stdout=subprocess.DEVNULL,
                )
            except subprocess.CalledProcessError as e:
                # the exception text contains the whole command line, i.e. the password
                raise RuntimeError(f"7z failed with exit code {e.returncode} while zipping {folders}") from None
            size_gb = os.path.getsize(warehouse_zip_path) / 1e9
            print(f"✅ [Part 2] Zipped at: {warehouse_zip_path} ({size_gb:.2f} GB)")
            print(f"⏱️  [Part 2] Zipping completed in {time.time() - zip_start:.2f}s")

            print("\n📤 [Part 2] Syncing warehouse reports to cloud...")
            sync_start = time.time()
            sync_reports_splat(warehouse_zip_path, config.fullReportSyncPath, config)
            print(f"⏱️  [Part 2] Sync completed in {time.time() - sync_start:.2f}s")
            # only reached if the sync succeeded
            os.remove(warehouse_zip_path)
            print(f"🗑️  [Part 2] Deleted local zip {warehouse_zip_path}")
        else:
            print("⚠️  [Part 2] None of the selected folders exist - nothing to zip")

        elapsed = time.time() - part2_start
        print(f"\n⏱️  Part 2 completed in {elapsed:.2f}s")
        return elapsed

    def upload_parquet_files(self, config):
        try:
            print("Starting upload of Parquet files to GCP bucket")

            base_path = config.unifiedParquetLocalPath
            user_details_file  = os.path.join(base_path, "unified_user_details.parquet")
            enrolments_file    = os.path.join(base_path, "unified_enrolments.parquet")
            org_hierarchy_file = os.path.join(base_path, "org_hierarchy.parquet")

            user_details_exists  = os.path.isfile(user_details_file)
            enrolments_exists    = os.path.isfile(enrolments_file)
            org_hierarchy_exists = os.path.isfile(org_hierarchy_file)

            if not user_details_exists:
                print(f"WARNING: File not found: {user_details_file}")
            if not enrolments_exists:
                print(f"WARNING: File not found: {enrolments_file}")
            if not org_hierarchy_exists:
                print(f"WARNING: File not found: {org_hierarchy_file}")

            if user_details_exists and enrolments_exists and org_hierarchy_exists:
                sync_reports_parallel(base_path, config.unifiedParquetPath, config)
                print("Completed uploading Parquet files to GCP bucket.")
            else:
                print("Upload skipped: One or more required files are missing.")

        except Exception as e:
            print(f"Error uploading Parquet files: {str(e)}")
            raise

    @staticmethod
    def zip_mdoid_folder(args):
        import pyminizip

        mdoid_folder, merged_dir, password = args
        mdoid_path = os.path.join(merged_dir, mdoid_folder)
        if not os.path.isdir(mdoid_path):
            return None
        zip_path  = str(os.path.join(mdoid_path, "reports.zip"))
        csv_files = [f for f in os.listdir(mdoid_path) if f.endswith(".csv")]
        if csv_files:
            try:
                file_paths   = [str(os.path.join(mdoid_path, f)) for f in csv_files]
                prefix_paths = [""] * len(file_paths)
                pyminizip.compress_multiple(
                    file_paths,
                    prefix_paths,
                    zip_path,
                    str(password),
                    5
                )
                for f in file_paths:
                    os.remove(f)
                return f"✓ Zipped {mdoid_folder} ({len(csv_files)} files)"
            except Exception as e:
                print(f"❌ EXCEPTION during zipping {mdoid_folder}: {str(e)}")
                return f"✗ Failed {mdoid_folder}: {str(e)}"
        return None

    @staticmethod
    def convert_parquet_to_csv_splat(task):
        folder, warehouse_base, warehouse_output_dir = task
        src = os.path.join(warehouse_base, folder)
        out = os.path.join(warehouse_output_dir, f"{folder}.csv")

        # all part files, skips Spark's _SUCCESS / .crc files
        lf = pl.scan_parquet(os.path.join(src, "**", "*.parquet"), hive_partitioning=True)

        # CSV can't hold nested columns -> convert struct/list columns to text
        schema = lf.collect_schema()
        fixes = []
        for name, dtype in schema.items():
            if isinstance(dtype, pl.Struct):
                fixes.append(pl.col(name).struct.json_encode())
            elif isinstance(dtype, pl.List):
                fixes.append(pl.col(name).list.eval(pl.element().cast(pl.String)).list.join(","))
        if fixes:
            lf = lf.with_columns(fixes)
        lf.sink_csv(out)          # single CSV, streamed
        return f"✅ {folder} -> {out}"
    @staticmethod
    def convert_parquet_to_csv(args):
        """
        Convert a parquet folder to a single CSV using pandas,
        reading one parquet file at a time to avoid OOM.
        """
        folder, warehouse_base, warehouse_output_dir = args
        folder_path = os.path.join(warehouse_base, folder)

        if not os.path.isdir(folder_path):
            return None

        parquet_files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]
        if not parquet_files:
            return None

        csv_output = os.path.join(warehouse_output_dir, f"{folder}.csv")

        try:
            first = True
            for pf in parquet_files:
                full_path = os.path.join(folder_path, pf)
                df = pd.read_parquet(full_path)
                df.to_csv(
                    csv_output,
                    index=False,
                    mode="w" if first else "a",
                    header=first,
                )
                del df
                first = False
            return f"✓ Converted {folder}"
        except Exception as e:
            return f"✗ Error processing {folder}: {str(e)}"

    @staticmethod
    def copy_file_to_mdoid(args):
        source_file, dest_dir, dest_filename = args
        try:
            os.makedirs(dest_dir, exist_ok=True)
            dest_path = os.path.join(dest_dir, dest_filename)
            if os.path.lexists(dest_path):              # same overwrite behaviour as copy2
                os.remove(dest_path)
            try:
                os.link(source_file, dest_path)         # hardlink: instant, no data is copied
                return "linked"
            except OSError:                             # other filesystem / links not allowed -> real copy
                shutil.copy2(source_file, dest_path)
                return "copied"
        except Exception as e:
            print(f"Error copying {source_file}: {e}")
            return False

    def process_data(self, spark, config):
        try:
            overall_start_time = time.time()
            today = self.get_date()
            print("📊 Loading and filtering data...")
            spark = SparkSession.getActiveSession()
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

            # ── Read org hierarchy for L0-based password grouping ──────────────
            print("📥 Reading org hierarchy...")
            org_hierarchy_df = spark.read.parquet(
                f"{config.warehouseReportDir}/{config.dwOrgTable}"
            ).select("mdo_id", "ministry_id", "department_id").cache()
            print(f"  Org hierarchy rows: {org_hierarchy_df.count()}")
            def _timed_parquet_upload():
                t = time.time()
                self.upload_parquet_files(config)
                return time.time() - t
            parquet_pool   = ThreadPoolExecutor(max_workers=1)
            parquet_future = parquet_pool.submit(_timed_parquet_upload)

            # ------------------ Part 1: Merge & Zip MDOID Reports ------------- #
            part1_start = time.time()
            print("\n" + "=" * 70)
            print("PART 1: MDOID REPORTS PROCESSING")
            print("=" * 70)

            base_dir                  = os.path.join(config.localReportDir, config.prefixDirectoryPath)
            directories_to_select     = config.pysparkDirectoriesToSelect
            cbp_directories_to_select = config.pysparkCBPDirectoriesToSelect
            today_date                = datetime.today().strftime('%Y-%m-%d')
            merged_dir                = os.path.join(config.localReportDir, config.destinationDirectoryPath)
            kcm_dir                   = os.path.join(base_dir, "kcm-report", today_date, "ContentCompetencyMapping")

            kcm_file = None
            if os.path.exists(kcm_dir):
                kcm_files = glob.glob(os.path.join(kcm_dir, "*.csv"))
                kcm_file  = kcm_files[0] if kcm_files else None

            if os.path.exists(merged_dir):
                shutil.rmtree(merged_dir)
            os.makedirs(merged_dir)

            print("\n📤 Syncing CBP report folders to cloud...")
            cbp_sync_start = time.time()
            for subfolder in cbp_directories_to_select:
                cbp_dir = os.path.join(base_dir, subfolder, today_date)
                if os.path.exists(cbp_dir):
                    print(f"  → Syncing: {cbp_dir}")
                    try:
                        sync_reports_parallel(cbp_dir, os.path.join(config.prefixDirectoryPath, subfolder, today_date), config)
                    except Exception as e:
                        print(f"✗ Failed syncing CBP folder {subfolder}: {e}")
                else:
                    print(f"⚠️ CBP directory not found: {cbp_dir}")
            print(f"⏱️  CBP sync completed in {time.time() - cbp_sync_start:.2f}s")

            all_mdoids = set()
            print("\n📁 Collecting CSV files from report directories...")
            collection_start = time.time()
            copy_tasks = []
            print(f"Copying of mdoid folders started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            for subfolder in directories_to_select:
                report_dir = os.path.join(base_dir, subfolder, today_date)
                if os.path.exists(report_dir):
                    for item in os.listdir(report_dir):
                        item_path = os.path.join(report_dir, item)

                        if item.startswith("mdoid="):
                            if item.endswith(".csv"):
                                mdoid_value = item.replace(".csv", "")
                            else:
                                mdoid_value = item

                            all_mdoids.add(mdoid_value)
                            dest_dir = os.path.join(merged_dir, mdoid_value)

                            if os.path.isdir(item_path):
                                for f in os.listdir(item_path):
                                    if f.endswith(".csv"):
                                        source = os.path.join(item_path, f)
                                        copy_tasks.append((source, dest_dir, f))
                            elif item_path.endswith(".csv"):
                                copy_tasks.append((item_path, dest_dir, f"{subfolder}.csv"))

            print(f"  Found {len(copy_tasks)} files to copy across {len(all_mdoids)} MDOID folders")

            if copy_tasks:
                print(f"  Copying files in parallel (max_workers=16)...")
                copy_start = time.time()
                with ThreadPoolExecutor(max_workers=16) as executor:
                    results = list(executor.map(self.copy_file_to_mdoid, copy_tasks))
                print(f"linked {results.count('linked'):,}, copied {results.count('copied'):,}, failed {results.count(False):,}")

            print(f"Copying of mdoid folders completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # ── Generate L0-grouped passwords ──────────────────────────────────
            mdoid_folders = [
                f for f in os.listdir(merged_dir)
                if os.path.isdir(os.path.join(merged_dir, f))
            ]

            print(f"\n🔑 Generating L0-grouped passwords for {len(mdoid_folders)} MDOID folders...")
            mdoid_password_map, password_df = self.generate_mdoid_passwords(
                spark, config, mdoid_folders, org_hierarchy_df
            )

            print("\n📋 MDO ID → Password mapping (sample):")
            password_df.show(10, truncate=False)

            # Store in Redis — all children of same L0 will have identical password
            rows = password_df.collect()
            key_value_map = {
                f"CB_EXT_{row['mdo_id'].replace('mdoid=', '')}_password": row['password']
                for row in rows
            }
            Redis.bulk_update(
                key_value_map=key_value_map,
                host=config.redisKpHost,
                port=config.redisPort,
                db='0'
            )
            print(f"✅ L0-grouped password mapping saved to Redis ({len(key_value_map)} keys)")

            # ── Password-protected ZIP per mdoid folder ────────────────────────
            print(f"\n🗜️  Creating password-protected ZIP files for {len(mdoid_folders)} MDOID folders...")
            print(f"Operational reports zipping started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            zip_start = time.time()

            zip_tasks = [
                (folder, merged_dir, mdoid_password_map[folder])
                for folder in mdoid_folders
                if folder in mdoid_password_map
            ]
            zip_folders_parallel(zip_tasks)
            print(f"✅ All MDOID folders zipped with L0-grouped passwords at: {merged_dir}")
            print(f"⏱️  Zipping completed in {time.time() - zip_start:.2f}s")
            print(f"Zipping of operational reports completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            print("\n📤 Syncing MDOID reports to cloud...")
            mdoid_sync_start = time.time()
            sync_reports_parallel(merged_dir, config.mdoReportSyncPath, config)
            print(f"⏱️  MDOID sync completed in {time.time() - mdoid_sync_start:.2f}s")

            print(f"\n📤 Syncing KCM file separately to: {config.kcmSyncPath}")
            try:
                kcm_sync_start = time.time()
                sync_reports_parallel(kcm_file, config.kcmSyncPath, config)
                print(f"⏱️  KCM sync completed in {time.time() - kcm_sync_start:.2f}s")
            except Exception as e:
                print(f"⚠️ WARNING: Failed to sync KCM file separately: {e}")

            print(f"\n⏱️  Part 1 completed in {time.time() - part1_start:.2f}s")

            print("\n📤 Waiting for the unified Parquet upload (started in the background)...")
            wait_start = time.time()
            upload_seconds = parquet_future.result()    # re-raises if the upload failed, so the job still fails
            parquet_pool.shutdown()
            print(f"⏱️  Parquet upload completed in {upload_seconds:.2f}s (background); main thread waited {time.time() - wait_start:.2f}s")
            part2_pool   = ThreadPoolExecutor(max_workers=1)
            part2_future = part2_pool.submit(self.run_full_report, config) if config.createFullReport else None
            # ── Unpersist ──────────────────────────────────────────────────────
            org_hierarchy_df.unpersist()

            
            if part2_future is not None:
                print("\n⏳ Waiting for Part 2 (full report, started in the background)...")
                wait_start = time.time()
                part2_seconds = part2_future.result()     # re-raises if Part 2 failed, so the job still fails
                print(f"⏱️  Part 2 took {part2_seconds:.2f}s (background); main thread waited {time.time() - wait_start:.2f}s")
            else:
                print("PART 2: SKIPPED (createFullReport flag is disabled)")
            part2_pool.shutdown()


        except Exception as e:
            print(f"✗ Error: {str(e)}")
            raise


def main():
    spark = SparkSession.builder \
        .appName("Zip Upload Model") \
        .config("spark.master", "local[28]") \
        .config("spark.driver.memory", "180g") \
        .config("spark.driver.memoryOverhead", "24g") \
        .config("spark.driver.maxResultSize", "12g") \
        .config("spark.sql.shuffle.partitions", "224") \
        .config("spark.sql.files.maxPartitionBytes", "256MB") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    config_dict = get_environment_config()
    config      = create_config(config_dict)

    start_time = datetime.now()
    print(f"[START] ZipUpload processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[CONFIG] createFullReport flag: {getattr(config, 'createFullReport', False)}")

    model = ZipUploadModel()
    model.process_data(spark, config)

    end_time = datetime.now()
    print(f"\n[END] ZipUpload completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {end_time - start_time}")
    spark.stop()


if __name__ == "__main__":
    main()
