import concurrent.futures
import fcntl
import glob
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))

from jobs.config import get_environment_config
from jobs.default_config import create_config

LOCK_FILE = "/tmp/clickhouse_warehouse_load.lock"      # the same lock as clickhouse_bash.sh: the two never run together
STREAMS = int(os.environ.get("STREAMS", "2"))
ONLY_TABLES = [t.strip() for t in os.environ.get("ONLY_TABLES", "").split(",") if t.strip()]
ACCEPT_INVALID_CERT = os.environ.get("ACCEPT_INVALID_CERT", "1") == "1"    # the servers use a self-signed certificate
DUCKDB_MEMORY, DUCKDB_THREADS = "64GB", 20
ATTEMPTS = 2

# ---------------------------------------------------------------------------------------------------------------
# The DuckDB query that builds each table from the parquet files of its warehouse folder (${SOURCE} is that folder).
# The same queries as clickhouse_bash.sh; tables that are loaded as they are have no query.
# ---------------------------------------------------------------------------------------------------------------
# user_detail: reads the warehouse folder "user_detail"
USER_DETAIL_SQL = """
 SELECT
     user_id,
     mdo_id,
     CAST(status AS INTEGER) AS status,
     CAST(no_of_karma_points AS INTEGER) AS no_of_karma_points_val,
     full_name,
     designation,
     email,
     phone_number,
     pincode,
     groups,
     tag,
     profile_status,
     user_registration_date,
     profile_last_updated_date,
     roles,
     gender,
     category,
     marked_as_not_my_user,
     is_verified_karmayogi,
     created_by_id,
     external_system,
     external_system_id,
     CAST(weekly_claps_day_before_yesterday AS INTEGER),
     CAST(total_event_learning_hours AS DOUBLE),
     CAST(total_content_learning_hours AS DOUBLE),
     CAST(total_learning_hours AS DOUBLE),
     employee_id,
     cadre,
     civil_service_type,
     civil_services,
     cadre_batch,
     is_on_central_deputation,
     is_from_organised_service_of_govt,
     data_last_generated_on,
     CAST(total_badges_earned AS BIGINT)
 FROM read_parquet('${SOURCE}/*.snappy.parquet')
 """

# content: reads the warehouse folder "content"
CONTENT_SQL = """
 SELECT
     content_id,
     content_provider_id,
     content_provider_name,
     content_name,
     content_type,
     batch_id,
     batch_name,
     batch_start_date,
     batch_end_date,
     content_duration,
     TRY_CAST(content_rating AS FLOAT) AS content_rating,
     last_published_on,
     content_retired_on,
     content_status,
     TRY_CAST(resource_count AS INTEGER) AS resource_count,
     TRY_CAST(total_certificates_issued AS INTEGER) AS total_certificates_issued,
     content_substatus,
     language,
     content_sub_type,
     scorm_flag,
     difficulty_level,
     data_last_generated_on
 FROM (
     SELECT *,
            ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY last_published_on DESC) AS rn
     FROM read_parquet('${SOURCE}/*.snappy.parquet')
 )
 WHERE rn = 1
 """

# assessment_detail: reads the warehouse folder "assessment_detail"
ASSESSMENT_DETAIL_SQL = """
 SELECT
     user_id,
     content_id,
     assessment_id,
     assessment_name,
     assessment_type,
     assessment_sub_type,
     assessment_duration,
     time_spent_by_the_user,
     completion_date,
     TRY_CAST(score_achieved AS FLOAT) AS score_achieved,
     TRY_CAST(overall_score AS FLOAT) AS overall_score,
     TRY_CAST(cut_off_percentage AS FLOAT) AS cut_off_percentage,
     TRY_CAST(total_question AS INTEGER) AS total_question,
     TRY_CAST(number_of_incorrect_responses AS INTEGER) AS number_of_incorrect_responses,
     TRY_CAST(number_of_retakes AS INTEGER) AS number_of_retakes,
     pass,
     data_last_generated_on
 FROM read_parquet('${SOURCE}/*.snappy.parquet')
 WHERE content_id IS NOT NULL
 """

# bp_enrolments: reads the warehouse folder "bp_enrolments"
BP_ENROLMENTS_SQL = """
 SELECT
     user_id,
     content_id,
     batch_id,
     batch_location,
     component_name,
     component_id,
     component_type,
     component_mode,
     component_status,
     component_duration,
     TRY_CAST(component_progress_percentage AS FLOAT) AS component_progress_percentage,
     TRY_CAST(component_completed_on AS DATE) AS component_completed_on,
     TRY_CAST(last_accessed_on AS DATE) AS last_accessed_on,
     TRY_CAST(offline_session_date AS DATE) AS offline_session_date,
     offline_session_start_time,
     offline_session_end_time,
     offline_attendance_status,
     instructors_name,
     program_coordinator_name,
     data_last_generated_on
 FROM read_parquet('${SOURCE}/*.snappy.parquet')
 WHERE content_id IS NOT NULL
   AND user_id IS NOT NULL
   AND batch_id IS NOT NULL
 """

# content_resource: reads the warehouse folder "content_resource"
CONTENT_RESOURCE_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.snappy.parquet')
 """

# cb_plan: reads the warehouse folder "cb_plan"
CB_PLAN_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.snappy.parquet')
 """

# org_hierarchy: reads the warehouse folder "org_hierarchy"
ORG_HIERARCHY_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.snappy.parquet')
 """

# kcm_content_mapping: reads the warehouse folder "kcm_content_mapping"
KCM_CONTENT_MAPPING_SQL = """
 SELECT
     course_id,
     competency_area_id,
     competency_theme_id,
     competency_sub_theme_id,
     data_last_generated_on
 FROM read_parquet('${SOURCE}/*.snappy.parquet')
 """

# kcm_dictionary: reads the warehouse folder "kcm_dictionary"
KCM_DICTIONARY_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.snappy.parquet')
 """

# events: reads the warehouse folder "event_details"
EVENTS_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.snappy.parquet')
 """

# events_enrolment: reads the warehouse folder "event_enrolment_details"
EVENTS_ENROLMENT_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.snappy.parquet')
 """

# user_enrolments: reads the warehouse folder "user_enrolments"
USER_ENROLMENTS_SQL = """
 SELECT
     user_id,
     batch_id,
     content_id,
     enrolled_on,
     TRY_CAST(content_progress_percentage AS FLOAT) AS content_progress_percentage,
     TRY_CAST(resource_count_consumed AS INTEGER) AS resource_count_consumed,
     user_consumption_status,
     first_completed_on,
     first_certificate_generated_on,
     last_completed_on,
     last_certificate_generated_on,
     content_last_accessed_on,
     certificate_generated,
     TRY_CAST(number_of_certificate AS INTEGER) AS number_of_certificate,
     TRY_CAST(user_rating AS FLOAT) AS user_rating,
     certificate_id,
     CASE
         WHEN live_cbp_plan_mandate = TRUE THEN TRUE
         WHEN live_cbp_plan_mandate = FALSE THEN FALSE
         WHEN lower(CAST(live_cbp_plan_mandate AS VARCHAR)) IN ('true','1','yes') THEN TRUE
         WHEN lower(CAST(live_cbp_plan_mandate AS VARCHAR)) IN ('false','0','no') THEN FALSE
         ELSE NULL
     END AS live_cbp_plan_mandate,
     data_last_generated_on,
     TRY_CAST(karma_points AS DOUBLE) AS karma_points,
     badge_id
 FROM read_parquet('${SOURCE}/*.snappy.parquet')
 WHERE content_id IS NOT NULL
 """

# content_custom: reads the warehouse folder "content_custom"
CONTENT_CUSTOM_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.parquet')
 """

# myigot: reads the warehouse folder "myigot"
MYIGOT_SQL = """
SELECT *
FROM read_parquet('${SOURCE}/*.parquet')
"""

# ai_cbp_fact: reads the warehouse folder "aiCBPFactTable"
AI_CBP_FACT_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.parquet')
 """

# learner_coverage_map: reads the warehouse folder "learnerCoverageMapTable"
LEARNER_COVERAGE_MAP_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.parquet')
 """

# funnel_stage_fact: reads the warehouse folder "funnelStageFactsTable"
FUNNEL_STAGE_FACT_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.parquet')
 """

# recommended_course_competency_map: reads the warehouse folder "recommendedCourseCompetencyMapTable"
RECOMMENDED_COURSE_COMPETENCY_MAP_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.parquet')
 """

# saved_course_map: reads the warehouse folder "savedCourseMapTable"
SAVED_COURSE_MAP_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.parquet')
 """

# org_dept_user_summary: reads the warehouse folder "orgDeptUserSummaryTable"
ORG_DEPT_USER_SUMMARY_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.parquet')
 """

# competency_master_list: reads the warehouse folder "competencyMasterListTable"
COMPETENCY_MASTER_LIST_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.parquet')
 """

# L_org_hierarchy: reads the warehouse folder "L_org_hierarchy"
L_ORG_HIERARCHY_SQL = """
 SELECT *
 FROM read_parquet('${SOURCE}/*.parquet')
 """


class ClickHouseLoadModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.ClickHouseLoad"
        self.config = None
        self.host = self.user = self.port = self.database = None
        self._env = None
        self.existing = None         # the tables that exist in the ClickHouse database (None = could not be listed)
        self.tmp_dir = None
        self.truncated = False       # set by load_into_clickhouse once it has truncated the current table
        self.prep_secs = self.load_secs = 0.0       # set by the load functions, read by _run_timed
        self.results = []            # (table, total seconds, prep seconds, load seconds, status), in loading order

    def name(self):
        return "ClickHouseLoad"

    def ch(self, sql, path=None):
        """One clickhouse-client call (native protocol over TLS). With a parquet file path, the file is streamed into the
        INSERT in sql (the client parses it); otherwise the output of the statement is returned. Raises RuntimeError with
        ClickHouse's own message."""
        cmd = ["clickhouse-client", "--host", self.host, "--port", str(self.port), "--user", self.user, "--secure",
               "--insert_deduplicate=0", "--date_time_input_format=best_effort"]
        if ACCEPT_INVALID_CERT:
            cmd.append("--accept-invalid-certificate")
        cmd += ["--query", sql]
        if path:
            with open(path, "rb") as f:
                p = subprocess.run(cmd, stdin=f, capture_output=True, text=True, env=self._env)
        else:
            p = subprocess.run(cmd, capture_output=True, text=True, env=self._env)
        if p.returncode != 0:
            raise RuntimeError(p.stderr.strip()[:300] or f"clickhouse-client exit code {p.returncode}")
        return p.stdout.strip()

    @staticmethod
    def duck_count(pattern):
        """Row count of a parquet glob."""
        con = duckdb.connect()
        try:
            return con.execute("SELECT count(*) FROM read_parquet(?)", [pattern]).fetchone()[0]
        finally:
            con.close()

    def load_into_clickhouse(self, table, folder, expected):
        """CORE, used by both loading modes: TRUNCATE the table, insert every parquet file of the folder over STREAMS parallel
        streams, verify the row count; on a failed insert or a count mismatch the whole load is repeated once.
        Returns True when the table holds exactly `expected` rows."""
        target = f"{self.database}.{table}"
        files = sorted(glob.glob(f"{folder}/*.parquet"))
        load_start, ok = time.time(), False
        for attempt in range(1, ATTEMPTS + 1):
            print(f"   ▶ attempt {attempt}: TRUNCATE + insert ({STREAMS} streams)", flush=True)
            try:
                self.ch(f"TRUNCATE TABLE {target}")
                self.truncated = True
            except RuntimeError as e:
                print(f"   ⚠️  TRUNCATE failed: {e}")
                continue
            failed = False
            with concurrent.futures.ThreadPoolExecutor(max_workers=STREAMS) as pool:
                inserts = [(f, pool.submit(self.ch, f"INSERT INTO {target} FORMAT Parquet", f)) for f in files]
                for f, insert in inserts:
                    try:
                        insert.result()
                    except Exception as e:  # noqa: BLE001
                        failed = True
                        print(f"   ⚠️  insert failed ({os.path.basename(f)}): {e}")
            if failed:
                continue
            try:
                actual = self.ch(f"SELECT count() FROM {target}")
            except RuntimeError:
                actual = "?"
            if str(actual) == str(expected):
                print(f"   ✔ row count OK: {actual}")
                ok = True
                break
            print(f"   ⚠️  row count mismatch: expected {expected}, got {actual}")
        self.load_secs = time.time() - load_start
        return ok

    def _run_timed(self, table, impl, *args):
        """Around one table: the banner, ONLY_TABLES, the check that the ClickHouse table exists, the timing and the result
        line. impl prepares the data and loads it: it raises when the data cannot be prepared (nothing is truncated) and
        returns whether the load was verified."""
        if ONLY_TABLES and table not in ONLY_TABLES:
            return
        turn = time.time()
        self.truncated, self.prep_secs, self.load_secs = False, 0.0, 0.0
        print("==============================")
        print(f"🚀 Loading {table} — started {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("==============================", flush=True)
        try:
            if self.existing is not None and table not in self.existing:
                raise RuntimeError(f"ClickHouse table {self.database}.{table} does not exist")
            ok = impl(table, *args)
        except Exception as e:  # noqa: BLE001
            print(f"   ❌ {str(e)[:300]} ({'the table may be empty or partial' if self.truncated else 'table NOT truncated'})")
            self.prep_secs = self.prep_secs or time.time() - turn
            ok = False
        el = time.time() - turn
        if ok:
            print(f"✅ Done {table} — took {int(el) // 60}m {int(el) % 60}s (prep {self.prep_secs:.0f}s, load {self.load_secs:.0f}s)", flush=True)
        else:
            print(f"❌ FAILED {table} after {int(el) // 60}m {int(el) % 60}s", flush=True)
        self.results.append((table, el, self.prep_secs, self.load_secs, "OK" if ok else "FAILED"))

    # MODE A: the parquet files of the warehouse folder are loaded as they are.   table = ClickHouse table, folder = warehouse folder
    def _load_direct_impl(self, table, folder):
        p0 = time.time()
        source = f"{self.config.warehouseReportDir}/{folder}"
        if not glob.glob(f"{source}/*.parquet"):
            raise RuntimeError(f"no parquet files in {source}")
        expected = self.duck_count(f"{source}/*.parquet")
        if expected <= 0:
            raise RuntimeError("source has 0 rows")
        self.prep_secs = time.time() - p0
        return self.load_into_clickhouse(table, source, expected)

    def load_direct(self, table, folder=None):
        self._run_timed(table, self._load_direct_impl, folder or table)

    # MODE B: DuckDB builds the data from the warehouse folder (casts, filters; ${SOURCE} in the query is the folder) and writes
    # it as split files of about 1 GB, which are then loaded in parallel.
    def _load_duckdb_impl(self, table, sql, folder):
        p0 = time.time()
        source = f"{self.config.warehouseReportDir}/{folder}"
        out = f"{self.tmp_dir}/{table}"
        shutil.rmtree(out, ignore_errors=True)
        con = duckdb.connect()
        try:
            con.execute(f"SET memory_limit = '{DUCKDB_MEMORY}'")
            con.execute(f"SET threads = {DUCKDB_THREADS}")
            con.execute(f"SET temp_directory = '{self.tmp_dir}/duckdb_spill'")
            con.execute(f"COPY ({sql.replace('${SOURCE}', source)}) TO '{out}' (FORMAT PARQUET, FILE_SIZE_BYTES '1GB')")
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"DuckDB step failed: {str(e)[:200]}") from None
        finally:
            con.close()
        if not glob.glob(f"{out}/*.parquet"):
            raise RuntimeError("DuckDB produced no files")
        expected = self.duck_count(f"{out}/*.parquet")
        if expected <= 0:
            raise RuntimeError("transformed source has 0 rows")
        self.prep_secs = time.time() - p0
        ok = self.load_into_clickhouse(table, out, expected)
        if ok:
            shutil.rmtree(out, ignore_errors=True)               # free the disk; the files of a failed table are kept
        return ok

    def load_table(self, table, sql, folder=None):
        self._run_timed(table, self._load_duckdb_impl, sql, folder or table)

    def process_data(self, config):
        """Loads every table. Returns True when all tables were loaded and verified and the cache was dropped."""
        try:
            self.config = config
            start_time = time.time()
            self.host = config.clickhouseIp
            self.user = config.clickhouseUser
            self.port = config.clickhousePort
            self.database = config.clickhouseDatabase
            self._env = dict(os.environ, CLICKHOUSE_PASSWORD=str(config.clickhousePassword))   # for clickhouse-client only
            warehouse_path = config.warehouseReportDir
            # every table name is read now: a missing attribute stops the run here, before any table is touched
            tables = [config.dwUserTable, config.dwCourseTable, config.dwAssessmentTable, config.dwBPEnrollmentsTable,
                      config.dwContentResourceTable, config.dwCBPlanTable, config.dwOrgTable, config.dwKcmContentTable,
                      config.dwKcmDictionaryTable, config.dwEventsTable, config.dwEventsEnrolmentTable, config.dwEnrollmentsTable]
            if not os.path.isdir(warehouse_path):
                raise RuntimeError(f"warehouse folder not found: {warehouse_path} (config.warehouseReportDir)")
            self.tmp_dir = f"{warehouse_path}/temp/clickhouse-test"
            os.makedirs(f"{self.tmp_dir}/duckdb_spill", exist_ok=True)
            print(f"🔗 ClickHouse: {self.host}:{self.port}/{self.database}  streams={STREAMS}")
            print(f"📁 Warehouse folder: {warehouse_path}")
            print(f"📋 Tables named by the config: {' '.join(tables)}")

            print("🔌 Checking the connection (nothing is truncated before this passes)...")
            try:
                self.ch("SELECT 1")
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(f"cannot connect to {self.host} as {self.user} (ip, port or password wrong in the config?). "
                                   f"Nothing was truncated. ({str(e)[:120]})") from None
            print("   ✔ connected")
            try:
                self.existing = set(self.ch("SELECT name FROM system.tables WHERE database = '"
                                            + self.database.replace("'", "''") + "'").split())
            except Exception:  # noqa: BLE001
                self.existing = None                           # no right to list the tables: nothing to check up front

            print("📚 Step 1: Loading User Details Data...")
            self.load_table(config.dwUserTable, USER_DETAIL_SQL)

            print("📚 Step 2: Loading Content Data...")
            self.load_table(config.dwCourseTable, CONTENT_SQL)

            print("📚 Step 3: Loading Assessment Data...")
            self.load_table(config.dwAssessmentTable, ASSESSMENT_DETAIL_SQL)

            print("📚 Step 4: Loading BP Enrolments Data...")
            self.load_table(config.dwBPEnrollmentsTable, BP_ENROLMENTS_SQL)

            print("📚 Step 5: Loading Content Resource Data...")
            self.load_table(config.dwContentResourceTable, CONTENT_RESOURCE_SQL)

            print("📚 Step 6: Loading CB Plan Data...")
            self.load_table(config.dwCBPlanTable, CB_PLAN_SQL)

            print("📚 Step 7: Loading Org Hierarchy Data...")
            self.load_table(config.dwOrgTable, ORG_HIERARCHY_SQL)

            print("📚 Step 8: Loading KCM Content Data...")
            self.load_table(config.dwKcmContentTable, KCM_CONTENT_MAPPING_SQL)

            print("📚 Step 9: Loading KCM Dict Data...")
            self.load_table(config.dwKcmDictionaryTable, KCM_DICTIONARY_SQL)

            print("📚 Step 10: Loading Events Data...")
            self.load_table(config.dwEventsTable, EVENTS_SQL, folder="event_details")

            print("📚 Step 11: Loading Events Enrolments Data...")
            self.load_table(config.dwEventsEnrolmentTable, EVENTS_ENROLMENT_SQL, folder="event_enrolment_details")

            print("📚 Step 12: Loading User Enrolments Data...")
            self.load_table(config.dwEnrollmentsTable, USER_ENROLMENTS_SQL)

            print("📚 Step 13: Loading User Enrolments Custom Data...")
            self.load_direct("user_enrolments_custom")

            print("📚 Step 14: Loading Content Custom Data...")
            self.load_table("content_custom", CONTENT_CUSTOM_SQL)

            print("📚 Step 15: Loading MyiGOT Data...")
            self.load_table("myigot", MYIGOT_SQL)

            print("📚 Step 16: Loading AI CBP Fact Data...")
            self.load_table("ai_cbp_fact", AI_CBP_FACT_SQL, folder="aiCBPFactTable")

            print("📚 Step 17: Loading Learner Coverage Map Data...")
            self.load_table("learner_coverage_map", LEARNER_COVERAGE_MAP_SQL, folder="learnerCoverageMapTable")

            print("📚 Step 18: Loading Funnel Stage Fact Data...")
            self.load_table("funnel_stage_fact", FUNNEL_STAGE_FACT_SQL, folder="funnelStageFactsTable")

            print("📚 Step 19: Loading Recommended Course Competency Map Data...")
            self.load_table("recommended_course_competency_map", RECOMMENDED_COURSE_COMPETENCY_MAP_SQL, folder="recommendedCourseCompetencyMapTable")

            print("📚 Step 20: Loading Saved Course Map Data...")
            self.load_table("saved_course_map", SAVED_COURSE_MAP_SQL, folder="savedCourseMapTable")

            print("📚 Step 21: Loading Org Dept User Summary Data...")
            self.load_table("org_dept_user_summary", ORG_DEPT_USER_SUMMARY_SQL, folder="orgDeptUserSummaryTable")

            print("📚 Step 22: Loading Competency Master List Data...")
            self.load_table("competency_master_list", COMPETENCY_MASTER_LIST_SQL, folder="competencyMasterListTable")

            print("📚 Step 23: Loading L Org Hierarchy Data...")
            self.load_table("L_org_hierarchy", L_ORG_HIERARCHY_SQL)

            print("📚 Step 24: Loading MDO Enrolment Fact Data...")
            self.load_direct("mdo_enrolment_fact")

            print("📚 Step 25: Loading MDO User Summary Data...")
            self.load_direct("mdo_user_summary")

            print("📚 Step 26: Loading User Access Data...")
            self.load_direct("user_access", folder="mdo_user_access")

            print("📚 Step 27: Loading Org Access Data...")
            self.load_direct("org_access", folder="mdo_org_access")

            print("🧹 Dropping the query cache...")
            cache_failed = False
            try:
                self.ch("SYSTEM DROP QUERY CACHE ON CLUSTER 'prod_cluster'")
            except Exception as e:  # noqa: BLE001
                print(f"   ❌ cache drop FAILED (dashboards may show stale tiles) ({str(e)[:100]})")
                cache_failed = True

            elapsed = time.time() - start_time
            print(f"\n🕒 Finished: {time.strftime('%Y-%m-%d %H:%M:%S')}   Total: {int(elapsed) // 60}m {int(elapsed) % 60}s\n")
            print("📊 PER-TABLE TIMING SUMMARY")
            print(f"{'Table':<36} {'Total':>9} {'Prep':>9} {'Load':>9}  Status")
            print(f"{'-' * 36} {'-' * 9} {'-' * 9} {'-' * 9}  ------")
            for table, el, prep_s, load_s, status in self.results:
                m = lambda s: f"{int(s) // 60}m {int(s) % 60:02d}s"  # noqa: E731
                print(f"{table:<36} {m(el):>9} {m(prep_s):>9} {m(load_s):>9}  {status}")
            failed_tables = [r[0] for r in self.results if r[4] != "OK"]
            unknown = [t for t in ONLY_TABLES if t not in {r[0] for r in self.results}]
            if failed_tables or unknown or cache_failed:
                print(f"\n❌ PROBLEMS: failed tables: {' '.join(failed_tables) or 'none'}; ONLY_TABLES names no table: "
                      f"{' '.join(unknown) or 'none'}; cache drop failed: {int(cache_failed)}")
                print("   A failed table may be empty or partial in prod — fix and run again before users arrive.")
                return False
            print("🎉 ALL TABLES LOADED AND VERIFIED")
            return True
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise


def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)

    lock = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("❌ Another load is already running")
        sys.exit(1)

    start_time = datetime.now()
    print(f"[START] ClickHouse load started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = ClickHouseLoadModel()
    success = model.process_data(config)
    end_time = datetime.now()
    print(f"[END] ClickHouse load completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {end_time - start_time}")
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()