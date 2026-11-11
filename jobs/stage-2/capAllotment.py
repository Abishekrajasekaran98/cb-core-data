import findspark

findspark.init()
import sys
from pathlib import Path
import os
import duckdb
import shutil
import time
from datetime import datetime

import psutil
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, from_json, explode, array_join, lower, regexp_replace, split, trim
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

sys.path.append(str(Path(__file__).resolve().parents[2]))

from jobs.config import get_environment_config
from jobs.default_config import create_config

# Same mapping chain as before, applied once per user group (not once per output row).
# NOTE (pre-existing behaviour, kept unchanged on purpose): the last REPLACE turns every
# 'user_id' produced by the earlier replaces into 'user_id_id'. Fix separately if unwanted.
ALLOTMENT_TYPE_MAPPED_SQL = """
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        allotment_type,
        'rootorgid', 'mdo_id'),
        'customuser', 'user_id'),
        'alluser', 'user_id'),
        'group', 'groups'),
        'batch', 'cadre_batch'),
        'service', 'civil_services'),
        'isprofileverified', 'is_verified_karmayogi'),
        'isoncentraldeputation', 'is_on_central_deputation'),
        'profilestatus', 'profile_status'),
        'user', 'user_id'
    )
"""


class CAPAccessControlModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.CAPAccessControlModel"

    def name(self):
        return "CAPAccessControlModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def current_date_time():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def get_access_control_schema(self):
        """Schema for parsing contextdata JSON"""
        return StructType([
            StructField("contentId", StringType(), True),
            StructField("accessControl", StructType([
                StructField("version", StringType(), True),
                StructField("userGroups", ArrayType(StructType([
                    StructField("userGroupId", StringType(), True),
                    StructField("userGroupName", StringType(), True),
                    StructField("userGroupCriteriaList", ArrayType(StructType([
                        StructField("criteriaKey", StringType(), True),
                        StructField("criteriaValue", ArrayType(StringType()), True)
                    ])), True)
                ])), True)
            ]), True)
        ])

    def process_data(self, spark, conf):
        con = None
        temp_dir = None
        try:
            print("=" * 80)
            print("CAP Access Control Processing - single-pass DuckDB")
            print("=" * 80)

            start_time = time.time()
            t = time.time()

            warehouse_path = conf.warehouseReportDir
            output_path = getattr(conf, 'baseCachePath', '/home/analytics/pyspark/data-res/pq_files/cache_pq/')

            project_dir = str(Path(__file__).resolve().parents[3])
            temp_dir = f"{project_dir}/temp_cap_duckdb"
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)

            # ============================================================ 1. Spark: parse CAP criteria
            print(f"\n[1/4] Processing CAP access control data (Spark)...")
            print(f"  Temp directory: {temp_dir}")

            content_df = spark.read.parquet(f"{warehouse_path}/{conf.dwCourseTable}")
            access_settings_df = spark.read.parquet(f"{output_path}/accessControlSettings")

            cap_df = content_df.filter(
                (col("content_sub_type") == "Comprehensive Assessment Program") &
                (col("content_status") == "Live")
            ).select("content_id", "content_name", "content_status", "content_provider_id")

            cap_access_df = access_settings_df.filter(
                col("contextidtype") == "Comprehensive Assessment Program"
            ).join(
                cap_df,
                access_settings_df["contextid"] == cap_df["content_id"],
                "inner"
            )

            # Pre-process contextdata to wrap scalar criteriaValue in arrays
            cap_access_df = cap_access_df.withColumn(
                "contextdata",
                regexp_replace(
                    col("contextdata"),
                    '"criteriaValue":((?!\\[)(true|false|"[^"]*"|[0-9]+(?:\\.[0-9]+)?))',
                    '"criteriaValue":[$1]'
                )
            )

            cap_allocation_df = (cap_access_df
            .withColumn("context_data", from_json(col("contextdata"), self.get_access_control_schema()))
            .withColumn("userGroup", explode(col("context_data.accessControl.userGroups")))
            .withColumn(
                "criteria_keys",
                expr("transform(userGroup.userGroupCriteriaList, x -> lower(x.criteriaKey))")
            )
            .withColumn(
                "criteria_values",
                expr("transform(userGroup.userGroupCriteriaList, x -> concat_ws(', ', x.criteriaValue))")
            )
            .withColumn("allotment_type", array_join(col("criteria_keys"), "|"))
            .withColumn("allotment_to", array_join(col("criteria_values"), "|"))
            .withColumn(
                "org_id",
                expr("""
                        CASE
                        WHEN array_contains(criteria_keys, 'rootorgid') THEN
                            filter(criteria_values,
                            (value, idx) -> criteria_keys[idx] = 'rootorgid'
                            )[0]
                        ELSE NULL
                        END
                    """)
            )
            .select(
                col("content_id").alias("cap_id"),
                col("content_name").alias("cap_name"),
                col("content_provider_id").alias("created_by_id"),
                col("org_id"),
                col("allotment_type"),
                col("allotment_to"),
                col("content_status").alias("status")
            )
            )

            # Small, but used by several actions below (counts, meta write, criteria write):
            # cache it once instead of re-reading + re-parsing the JSON for each action.
            # Also makes the monotonically_increasing_id() below run on fixed input.
            cap_allocation_df = cap_allocation_df.persist(StorageLevel.MEMORY_AND_DISK)

            allocation_count = cap_allocation_df.count()
            meta_cap_count = cap_allocation_df.select("cap_id").distinct().count()
            print(f"  CAP allocations (userGroups): {allocation_count:,}")
            print(f"  Distinct CAPs in meta: {meta_cap_count:,}")

            cap_allocation_df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
                f"{conf.warehouseReportDir}/cap_allocation_meta")
            print(f"\nCAP allocation meta data written to warehouse folder")

            cap_allocation_with_group = cap_allocation_df.withColumn(
                "user_group_id",
                expr("concat(cap_id, '_', monotonically_increasing_id())")
            )

            cap_criteria_exploded = (cap_allocation_with_group
                                     .withColumn("criteria_types_arr", split(col("allotment_type"), "\\|"))
                                     .withColumn("criteria_values_arr", split(col("allotment_to"), "\\|"))
                                     .withColumn("criteria_idx", expr("sequence(0, size(criteria_types_arr) - 1)"))
                                     .withColumn("criteria_exploded", explode(col("criteria_idx")))
                                     .withColumn("criteria_type",
                                                 expr("lower(trim(criteria_types_arr[criteria_exploded]))"))
                                     .withColumn("criteria_value_raw", expr("criteria_values_arr[criteria_exploded]"))
                                     .select(
                "cap_id", "cap_name", "created_by_id", "org_id", "status",
                "user_group_id",
                "allotment_type", "allotment_to",
                "criteria_type", "criteria_value_raw"
            )
                                     .withColumn("criteria_value_clean", lower(trim(col("criteria_value_raw"))))
                                     )

            cap_criteria_path = f"{temp_dir}/cap_criteria.parquet"
            cap_criteria_exploded.write.mode("overwrite").parquet(cap_criteria_path)
            cap_allocation_df.unpersist()
            print(f"  Step time: {time.time() - t:.1f}s")

            # ============================================================ 2. DuckDB setup + lookups
            print("\n[2/4] Building criteria / group / user tables (DuckDB)...")
            t = time.time()

            user_data_path = f"{warehouse_path}/{conf.dwUserTable}"

            spill_dir = f"{temp_dir}/spill"
            os.makedirs(spill_dir, exist_ok=True)
            budget_gb = max(8, int(psutil.virtual_memory().available / (1024 ** 3) * 0.6))
            # In-memory DB (was a .duckdb file on disk); spills to spill_dir when needed.
            con = duckdb.connect()
            con.execute(f"SET temp_directory='{spill_dir}'")
            con.execute(f"SET memory_limit='{budget_gb}GB'")
            con.execute(f"SET threads={os.cpu_count() or 4}")
            con.execute("SET preserve_insertion_order=false")
            print(f"  DuckDB memory limit: {budget_gb}GB, threads: {os.cpu_count()}")

            # Criteria values are comma-separated -> one row per value (unchanged logic)
            con.execute(f"""
                CREATE TABLE criteria_exploded AS
                SELECT cap_id, user_group_id, criteria_type,
                       LOWER(TRIM(cv.value)) AS criteria_value
                FROM read_parquet('{cap_criteria_path}/**.parquet'),
                LATERAL (SELECT unnest(string_split(criteria_value_clean, ',')) AS value) cv
                WHERE LENGTH(TRIM(cv.value)) > 0
            """)

            # Per user group: how many NON-rootorgid criteria types must match.
            # (Old logic counted rootorgid as a type the user had to match too; here rootorgid is
            #  applied as an org filter instead, which gives the same qualifying users.)
            con.execute("""
                        CREATE TABLE group_req AS
                        SELECT cap_id, user_group_id,
                               COUNT(DISTINCT criteria_type) FILTER (WHERE criteria_type <> 'rootorgid') AS required_types
                        FROM criteria_exploded
                        GROUP BY cap_id, user_group_id
                        """)

            con.execute("""
                        CREATE TABLE group_org AS
                        SELECT DISTINCT cap_id, user_group_id, criteria_value AS org_value
                        FROM criteria_exploded
                        WHERE criteria_type = 'rootorgid'
                        """)

            # One row per (criterion value, allowed org); org_value NULL = group has no rootorgid.
            con.execute("""
                        CREATE TABLE attr_criteria AS
                        SELECT DISTINCT c.cap_id, c.user_group_id, c.criteria_type, c.criteria_value, o.org_value
                        FROM criteria_exploded c
                                 LEFT JOIN group_org o
                                           ON o.cap_id = c.cap_id AND o.user_group_id = c.user_group_id
                        WHERE c.criteria_type <> 'rootorgid'
                        """)

            # One row per user group (old version joined one row PER CRITERION and then needed a
            # big DISTINCT over the whole output to collapse the duplicates).
            con.execute(f"""
                CREATE TABLE group_info AS
                SELECT DISTINCT cap_id, user_group_id, cap_name, created_by_id,
                       {ALLOTMENT_TYPE_MAPPED_SQL} AS allotment_type,
                       allotment_to
                FROM read_parquet('{cap_criteria_path}/**.parquet')
            """)

            # Users read ONCE (old view re-scanned the user parquet in every one of ~12 UNION branches)
            con.execute(f"""
                CREATE TABLE users_base AS
                SELECT DISTINCT
                    user_id, full_name, email, phone_number, designation, groups, tag, cadre,
                    civil_services, cadre_batch, is_on_central_deputation,
                    LOWER(TRIM(COALESCE(user_id, '')))                                AS user_id_lower,
                    LOWER(TRIM(COALESCE(mdo_id, '')))                                 AS mdo_id_lower,
                    LOWER(TRIM(COALESCE(designation, '')))                            AS designation_lower,
                    LOWER(TRIM(COALESCE(groups, '')))                                 AS groups_lower,
                    LOWER(TRIM(COALESCE(tag, '')))                                    AS tag_lower,
                    LOWER(TRIM(COALESCE(cadre, '')))                                  AS cadre_lower,
                    LOWER(TRIM(COALESCE(civil_service_type, '')))                     AS civil_service_type_lower,
                    LOWER(TRIM(COALESCE(civil_services, '')))                         AS civil_services_lower,
                    LOWER(TRIM(COALESCE(cadre_batch, '')))                            AS cadre_batch_lower,
                    CASE
                        WHEN is_on_central_deputation IS NULL
                          OR TRIM(CAST(is_on_central_deputation AS VARCHAR)) = ''
                        THEN 'false'
                        ELSE LOWER(TRIM(CAST(is_on_central_deputation AS VARCHAR)))
                    END                                                               AS is_on_central_deputation_lower,
                    LOWER(TRIM(COALESCE(CAST(is_verified_karmayogi AS VARCHAR), ''))) AS is_verified_karmayogi_lower,
                    LOWER(TRIM(COALESCE(profile_status, '')))                         AS profile_status_lower
                FROM read_parquet('{user_data_path}/**.parquet')
                WHERE status = 1
            """)

            crit_rows = con.execute("SELECT COUNT(*) FROM criteria_exploded").fetchone()[0]
            group_rows = con.execute("SELECT COUNT(*) FROM group_req").fetchone()[0]
            user_count = con.execute("SELECT COUNT(*) FROM users_base").fetchone()[0]
            print(f"  Criteria rows:  {crit_rows:,}")
            print(f"  User groups:    {group_rows:,}")
            print(f"  Active users:   {user_count:,}")
            print(f"  Step time: {time.time() - t:.1f}s")

            # ============================================================ 3. Single-pass matching
            print("\n[3/4] Matching users to CAP user groups (single pass)...")
            t = time.time()

            # Every user unpivoted to (attr_type, attr_value): ONE hash join replaces ~12 queries.
            # The type -> user attribute pairs are exactly the ones the old UNION branches used.
            # For groups with rootorgid, the org is part of the join key, so only that org's users
            # are touched (old: tag / user / customuser / alluser had no org filter, but the group's
            # rootorgid still had to match, so the result is the same).
            con.execute("""
                        CREATE TABLE qualified_pairs AS
                        WITH user_attrs AS (
                            SELECT user_id, mdo_id_lower, attr_type, attr_value
                            FROM (
                                     SELECT user_id, mdo_id_lower,
                                            UNNEST(['user', 'customuser', 'alluser', 'designation', 'group', 'tag',
                                                            'cadre', 'civil_service_type', 'service', 'batch',
                                                            'isoncentraldeputation', 'isprofileverified', 'profilestatus']) AS attr_type,
                                            UNNEST([user_id_lower, user_id_lower, user_id_lower, designation_lower,
                                                                   groups_lower, tag_lower, cadre_lower, civil_service_type_lower,
                                                                   civil_services_lower, cadre_batch_lower, is_on_central_deputation_lower,
                                                                   is_verified_karmayogi_lower, profile_status_lower])              AS attr_value
                                     FROM users_base
                                 )
                            WHERE attr_value <> ''
                        ),
                             matches AS (
                                 SELECT ua.user_id, c.cap_id, c.user_group_id, c.criteria_type AS matched_type
                                 FROM user_attrs ua
                                          JOIN (SELECT * FROM attr_criteria WHERE org_value IS NOT NULL) c
                                               ON c.criteria_type  = ua.attr_type
                                                   AND c.criteria_value = ua.attr_value
                                                   AND c.org_value      = ua.mdo_id_lower

                                 UNION ALL

                                 SELECT ua.user_id, c.cap_id, c.user_group_id, c.criteria_type AS matched_type
                                 FROM user_attrs ua
                                          JOIN (SELECT * FROM attr_criteria WHERE org_value IS NULL) c
                                               ON c.criteria_type  = ua.attr_type
                                                   AND c.criteria_value = ua.attr_value
                             )
                        -- AND within a user group: every required criteria type matched
                        SELECT m.user_id, m.cap_id, m.user_group_id
                        FROM matches m
                                 JOIN group_req r
                                      ON r.cap_id = m.cap_id AND r.user_group_id = m.user_group_id
                        GROUP BY m.user_id, m.cap_id, m.user_group_id, r.required_types
                        HAVING COUNT(DISTINCT m.matched_type) = r.required_types

                        UNION ALL

                        -- groups whose ONLY criterion is rootorgid -> every active user of that org
                        SELECT DISTINCT u.user_id, r.cap_id, r.user_group_id
                        FROM group_req r
                                 JOIN group_org o
                                      ON o.cap_id = r.cap_id AND o.user_group_id = r.user_group_id
                                 JOIN users_base u ON u.mdo_id_lower = o.org_value
                        WHERE r.required_types = 0
                        """)

            qp_count = con.execute("SELECT COUNT(*) FROM qualified_pairs").fetchone()[0]
            print(f"  Qualified (user, CAP, user group) rows: {qp_count:,}")
            print(f"  Step time: {time.time() - t:.1f}s")

            # ============================================================ 4. Final output
            print("\n[4/4] Writing user-wise allocation...")
            t = time.time()

            # OR between user groups: DISTINCT on the narrow allocation columns (cheap) instead of
            # on the full output rows. Output columns and order unchanged.
            temp_output = f"{temp_dir}/cap_allocation_user_wise.parquet"
            con.execute(f"""
                COPY (
                    WITH alloc AS (
                        SELECT DISTINCT qp.user_id, g.cap_id, g.cap_name, g.created_by_id,
                                        g.allotment_type, g.allotment_to
                        FROM qualified_pairs qp
                        JOIN group_info g
                          ON g.cap_id = qp.cap_id AND g.user_group_id = qp.user_group_id
                    ),
                    users_out AS (
                        SELECT DISTINCT user_id, full_name, email, phone_number, designation, groups, tag,
                                        cadre, civil_services, cadre_batch, is_on_central_deputation
                        FROM users_base
                    )
                    SELECT
                        u.user_id, u.full_name, u.email, u.phone_number, u.designation, u.groups, u.tag,
                        u.cadre, u.civil_services, u.cadre_batch, u.is_on_central_deputation,
                        a.cap_id, a.cap_name, a.created_by_id,
                        a.allotment_type, a.allotment_to
                    FROM alloc a
                    JOIN users_out u ON u.user_id = a.user_id
                ) TO '{temp_output}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
            """)

            out_rows, out_caps = con.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT cap_id) FROM read_parquet('{temp_output}')"
            ).fetchone()
            print(f"  [VERIFICATION] Rows: {out_rows:,}   Distinct CAPs in user wise output: {out_caps:,}")

            # DuckDB writes the single output file directly (was: read back into Spark, then a
            # coalesce(1) write through one task). Swap in only after the file is complete.
            final_dir = f"{conf.warehouseReportDir}/cap_allocation_user_wise"
            shutil.rmtree(final_dir, ignore_errors=True)
            os.makedirs(final_dir, exist_ok=True)
            shutil.move(temp_output, f"{final_dir}/part-00000.parquet")
            print(f"  User wise allocation parquet written to {final_dir}")
            print(f"  Step time: {time.time() - t:.1f}s")

            elapsed_time = time.time() - start_time

            print("\n" + "=" * 80)
            print(f"PROCESSING COMPLETE - total {elapsed_time:.1f}s ({elapsed_time / 60:.1f} min)")
            print("=" * 80)
            print("\n[SUCCESS] CAP Access Control processing completed")

        except Exception as e:
            print(f"❌ Error occurred during CAPAccessControlModel processing: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            if con is not None:
                con.close()
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    # The Cassandra / Elasticsearch / Postgres connector packages were removed from
    # PYSPARK_SUBMIT_ARGS: this job reads only local parquet, and resolving those packages
    # cost startup time on every run.
    config_dict = get_environment_config()
    config = create_config(config_dict)

    spark = SparkSession.builder \
        .appName("CAP Access Control Model - DuckDB") \
        .config("spark.master", "local[*]") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.driver.memory", "10g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
        .getOrCreate()

    start_time = datetime.now()
    print(f"[START] CAPAccessControlModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    model = CAPAccessControlModel()
    model.process_data(spark, config)

    end_time = datetime.now()
    print(f"[END] CAPAccessControlModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {end_time - start_time}")
    spark.stop()


if __name__ == "__main__":
    main()
