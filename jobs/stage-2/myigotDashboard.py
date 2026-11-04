#!/usr/bin/env python3
"""
build_myigot_duckdb.py -- builds warehouse/myigot (the iGOT dashboard table) with DuckDB.
Same logic as BigQuery's CREATE TABLE kb_prod_dataset.myigot and the Spark job build_myigot_bq_logic:
every active user x course that a Live plan assigns to them, one row per user and course (the
latest plan wins), with enrolment, assessment, organisation and course columns.

Run:    python3 build_myigot_duckdb.py
Needs:  pip install duckdb
Exit code 0 = success; anything else = failed, existing output left unchanged.

Run AFTER build_l_org_hierarchy.py (it reads L_org_hierarchy).

How a user gets a course (as BigQuery)
  1. plans with status 'Live'; a plan with an empty org_id, or an org_id containing a comma, is a
     global plan (reaches every user); any other org_id reaches only the users of that organisation
  2. the user must be active (user_detail.status = 1)
  3. allotment rules: allotment_type and allotment_to are split on '|', each rule's values on ',';
     the user must satisfy EVERY rule through one of: designation, groups, civil_services, cadre,
     cadre_batch, user, mdo_id, is_on_central_deputation (compared in lower case, trimmed)
  4. several plans for the same user and course: the one with the latest allocated_on, then
     due_by, then cb_plan_id wins

Safeguards: one run at a time; input folders, columns and row counts checked; unreadable date
text counted (stops above MAX_BAD_DATE_PCT); result checked (exactly one row per user and course,
no big drop in rows compared with the current output); written to myigot_new first and only then
replaces myigot. The log prints the plan and result figures in the same layout as the BigQuery
check queries, so the two can be compared side by side.

Where this differs from the Spark job / BigQuery
  * dates are real UTC timestamps; text with a time-zone offset is converted correctly (the ranking
    of plans depends on allocated_on)
  * where BigQuery and Spark keep an arbitrary row among duplicates, this keeps a fixed one:
    several enrolments of a user in a course -> the latest enrolment (same timestamp: the one with a
    certificate id); several assessment attempts -> the best score; duplicate user_detail rows -> the
    first by mdo_id and email
  * plan rows that tie on allocated_on, due_by and cb_plan_id are ordered by plan_name then content_id
"""
import atexit
import fcntl
import os
import re
import shutil
import sys
import threading
import time

import duckdb

# ---------------- settings ----------------
WAREHOUSE        = "/home/analytics/pyspark/warehouse"
OUTPUT           = WAREHOUSE + "/myigot"
MEM              = "120GB"      # keep well below the server RAM (DuckDB uses some extra)
THREADS          = 12
TMP              = WAREHOUSE + "/temp/duckdb_myigot"   # work database and spill files
MAX_DROP_PCT     = 5            # refuse to replace the output if it has this % fewer rows than the current one
MAX_BAD_DATE_PCT = 0.5          # stop if more than this % of a date column's text cannot be read
HEARTBEAT_SECONDS = 60          # while a stage runs, print a status line this often
# ------------------------------------------

INPUTS = {   # folder -> columns that must exist
    "cb_plan": ["cb_plan_id", "content_id", "plan_name", "allocated_on", "due_by", "status", "org_id", "isapar",
                "allotment_type", "allotment_to"],
    "user_detail": ["user_id", "mdo_id", "status", "is_on_central_deputation", "full_name", "designation",
                    "is_from_organised_service_of_govt", "civil_service_type", "civil_services", "cadre_batch",
                    "cadre", "email", "phone_number", "groups", "roles"],
    "user_enrolments": ["user_id", "content_id", "user_consumption_status", "content_progress_percentage",
                        "certificate_id", "first_certificate_generated_on", "enrolled_on"],
    "L_org_hierarchy": ["mdo_id", "mdo_name", "ministry_mdo_id", "ministry", "department_mdo_id", "department",
                        "Centre_State"],
    "assessment_detail": ["user_id", "content_id", "assessment_sub_type", "score_achieved", "cut_off_percentage"],
    "content": ["content_id", "content_name", "content_duration", "content_sub_type"],
}

DATE_COLUMNS = {   # placeholder in the SQL -> (folder, column)
    "TS_PLAN_ALLOC": ("cb_plan", "allocated_on"),
    "TS_PLAN_DUE":   ("cb_plan", "due_by"),
    "TS_UEN_ENR":    ("user_enrolments", "enrolled_on"),
    "TS_UEN_FIRST":  ("user_enrolments", "first_certificate_generated_on"),
}

SUPPORTED_RULE_KEYS = {"designation", "groups", "civil_services", "cadre", "cadre_batch", "user", "mdo_id",
                       "is_on_central_deputation"}

SQL = r"""
SET memory_limit = '@@MEM@@';
SET threads = @@THREADS@@;
SET temp_directory = '@@TMP@@';
SET preserve_insertion_order = false;
SET TimeZone = 'UTC';

CREATE OR REPLACE VIEW cb_plan_src  AS SELECT * FROM read_parquet('@@W@@/cb_plan/*.parquet');
CREATE OR REPLACE VIEW user_src     AS SELECT * FROM read_parquet('@@W@@/user_detail/*.parquet');
CREATE OR REPLACE VIEW enrol_src    AS SELECT * FROM read_parquet('@@W@@/user_enrolments/*.parquet');
CREATE OR REPLACE VIEW org_src      AS SELECT * FROM read_parquet('@@W@@/L_org_hierarchy/*.parquet');
CREATE OR REPLACE VIEW assess_src   AS SELECT * FROM read_parquet('@@W@@/assessment_detail/*.parquet');
CREATE OR REPLACE VIEW content_src  AS SELECT * FROM read_parquet('@@W@@/content/*.parquet');

-- ---------------------------------------------------------------------------
-- Stage 1: Live plan rows, each with a priority number (1 = the plan that wins), its effective
--          organisation (NULL = global plan) and its rule set
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE cp AS
SELECT row_number() OVER (ORDER BY allocated_on DESC NULLS LAST, due_by DESC NULLS LAST,
                                   cb_plan_id DESC NULLS LAST, plan_name DESC NULLS LAST,
                                   content_id DESC NULLS LAST) AS prio, *
FROM (
    SELECT * REPLACE (@@TS_PLAN_ALLOC@@ AS allocated_on, @@TS_PLAN_DUE@@ AS due_by),
           CASE WHEN org_id IS NULL          THEN NULL
                WHEN trim(org_id) = ''       THEN NULL
                WHEN strpos(org_id, ',') > 0 THEN NULL
                ELSE trim(org_id) END                                   AS org_effective
    FROM cb_plan_src
    WHERE status = 'Live'
);

CREATE OR REPLACE TABLE rule_sets AS
SELECT row_number() OVER ()                                             AS rs_id,
       org_effective, allotment_type, allotment_to,
       string_split(lower(coalesce(allotment_type, '')), '|')           AS type_arr,
       string_split(coalesce(allotment_to, ''), '|')                    AS to_arr
FROM (SELECT DISTINCT org_effective, allotment_type, allotment_to FROM cp);

CREATE OR REPLACE TABLE course_ids AS
SELECT row_number() OVER () AS cid, content_id
FROM (SELECT DISTINCT content_id FROM cp);

CREATE OR REPLACE TABLE cp_rs AS
SELECT cp.*, rs.rs_id, ci.cid
FROM cp
JOIN course_ids ci ON ci.content_id IS NOT DISTINCT FROM cp.content_id
JOIN rule_sets rs
  ON rs.org_effective  IS NOT DISTINCT FROM cp.org_effective
 AND rs.allotment_type IS NOT DISTINCT FROM cp.allotment_type
 AND rs.allotment_to   IS NOT DISTINCT FROM cp.allotment_to;

-- one row per (rule, allowed value); a rule without values produces no rows, so it can never be met
CREATE OR REPLACE TABLE allowed AS
SELECT DISTINCT rs_id, org_effective, n_rules, i, rule_key,
       lower(trim(replace(v, '"', '')))                                 AS allowed_value
FROM (
    SELECT rs_id, org_effective, n_rules, i, rule_key, unnest(string_split(to_part, ',')) AS v
    FROM (
        SELECT rs_id, org_effective, len(type_arr) AS n_rules, i,
               lower(trim(type_arr[i]))                                 AS rule_key,
               to_arr[i]                                                AS to_part
        FROM (SELECT *, unnest(generate_series(1, len(type_arr))) AS i FROM rule_sets)
    )
)
WHERE trim(replace(v, '"', '')) != '';

-- ---------------------------------------------------------------------------
-- Stage 2: active users (one row each) and the attributes the rules can test
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE ud AS
SELECT row_number() OVER () AS uid, *
FROM (SELECT DISTINCT ON (user_id) *
      FROM user_src
      WHERE TRY_CAST(status AS INTEGER) = 1
      ORDER BY user_id, mdo_id, email);

CREATE OR REPLACE TABLE user_dims AS
SELECT uid, mdo_id AS user_mdo, dim_key, dim_value FROM (
    SELECT uid, mdo_id, 'designation'    AS dim_key, lower(trim(CAST(designation    AS VARCHAR))) AS dim_value FROM ud
    UNION ALL SELECT uid, mdo_id, 'groups',         lower(trim(CAST("groups"       AS VARCHAR))) FROM ud
    UNION ALL SELECT uid, mdo_id, 'civil_services', lower(trim(CAST(civil_services AS VARCHAR))) FROM ud
    UNION ALL SELECT uid, mdo_id, 'cadre',          lower(trim(CAST(cadre          AS VARCHAR))) FROM ud
    UNION ALL SELECT uid, mdo_id, 'cadre_batch',    lower(trim(CAST(cadre_batch    AS VARCHAR))) FROM ud
    UNION ALL SELECT uid, mdo_id, 'user',           lower(trim(CAST(user_id        AS VARCHAR))) FROM ud
    UNION ALL SELECT uid, mdo_id, 'mdo_id',         lower(trim(CAST(mdo_id         AS VARCHAR))) FROM ud
    UNION ALL SELECT uid, mdo_id, 'is_on_central_deputation',
                     lower(trim(CAST(is_on_central_deputation AS VARCHAR))) FROM ud
              WHERE is_on_central_deputation IS NOT NULL
)
WHERE dim_value IS NOT NULL
  AND dim_key IN (SELECT DISTINCT rule_key FROM allowed);

-- ---------------------------------------------------------------------------
-- Stage 3: (rule set, user) pairs where the user meets EVERY rule
--          organisation plans: only users of that organisation; global plans: any user
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE eligible AS
SELECT rs_id, uid
FROM (
    SELECT DISTINCT a.rs_id, a.n_rules, a.i, d.uid
    FROM allowed a
    JOIN user_dims d ON d.dim_key = a.rule_key AND d.dim_value = a.allowed_value AND d.user_mdo = a.org_effective
    WHERE a.org_effective IS NOT NULL
    UNION ALL
    SELECT DISTINCT a.rs_id, a.n_rules, a.i, d.uid
    FROM allowed a
    JOIN user_dims d ON d.dim_key = a.rule_key AND d.dim_value = a.allowed_value
    WHERE a.org_effective IS NULL
)
GROUP BY rs_id, uid, n_rules
HAVING count(*) = n_rules;

DROP TABLE user_dims;

-- ---------------------------------------------------------------------------
-- Stage 4: the winning plan row for each (user, course) = the lowest priority number
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE ranked AS
SELECT e.uid, c.cid, min(c.prio) AS prio
FROM cp_rs c
JOIN eligible e ON e.rs_id = c.rs_id
GROUP BY e.uid, c.cid;

DROP TABLE eligible;

-- ---------------------------------------------------------------------------
-- Stage 5: one row per key for every joined source (plan courses and active users only)
-- ---------------------------------------------------------------------------
-- enrolments: the latest enrolment per (user, course); on the same timestamp the one with a certificate id wins
CREATE OR REPLACE TABLE ue1 AS
SELECT uid, cid,
       struct_extract(s, 'user_consumption_status')        AS user_consumption_status,
       struct_extract(s, 'content_progress_percentage')    AS content_progress_percentage,
       struct_extract(s, 'certificate_id')                 AS certificate_id,
       struct_extract(s, 'first_certificate_generated_on') AS first_certificate_generated_on,
       struct_extract(s, 'enrolled_on')                    AS enrolled_on
FROM (
    SELECT ud.uid, ci.cid,
           arg_max(struct_pack(
                       user_consumption_status        := e.user_consumption_status,
                       content_progress_percentage    := e.content_progress_percentage,
                       certificate_id                 := e.certificate_id,
                       first_certificate_generated_on := @@TS_UEN_FIRST@@,
                       enrolled_on                    := @@TS_UEN_ENR@@),
                   struct_pack(t := coalesce(epoch_us(@@TS_UEN_ENR@@), -9000000000000000000),
                               c := coalesce(CAST(e.certificate_id AS VARCHAR), ''),
                               u := coalesce(CAST(e.user_consumption_status AS VARCHAR), '')))  AS s
    FROM enrol_src e
    JOIN course_ids ci ON ci.content_id = e.content_id
    JOIN ud            ON ud.user_id    = e.user_id
    GROUP BY ud.uid, ci.cid
);

-- assessments of the two counted types: the best score per (user, course); on the same score, the later type name.
-- Only for (user, course) pairs that have an enrolment (as BigQuery joins assessments through the enrolment); doing
-- it here keeps the final join a plain equality join, which every DuckDB version runs as a fast hash join
CREATE OR REPLACE TABLE asd1 AS
SELECT x.uid, x.cid,
       struct_extract(x.s, 'assessment_sub_type') AS assessment_sub_type,
       struct_extract(x.s, 'score_achieved')      AS score_achieved,
       struct_extract(x.s, 'cut_off_percentage')  AS cut_off_percentage
FROM (
    SELECT ud.uid, ci.cid,
           arg_max(struct_pack(assessment_sub_type := a.assessment_sub_type,
                               score_achieved      := a.score_achieved,
                               cut_off_percentage  := a.cut_off_percentage),
                   struct_pack(sc := coalesce(TRY_CAST(a.score_achieved AS DOUBLE), -1e300),
                               t  := coalesce(CAST(a.assessment_sub_type AS VARCHAR), ''),
                               ct := coalesce(CAST(a.cut_off_percentage AS VARCHAR), '')))  AS s
    FROM assess_src a
    JOIN course_ids ci ON ci.content_id = a.content_id
    JOIN ud            ON ud.user_id    = a.user_id
    WHERE a.assessment_sub_type IN ('Course Assessment', 'Comprehensive Assessment Program')
    GROUP BY ud.uid, ci.cid
) x
JOIN ue1 e ON e.uid = x.uid AND e.cid = x.cid;

-- duration_hours = BigQuery's ROUND(hours + minutes / 60 + seconds / 3600, 2) on FLOAT64, worked out once per course.
-- BigQuery rounds the exact value the double is stored as; DuckDB's own round() rounds some exact ties up, so the
-- double's exact digits are printed and rounded instead. NULL when any of the three parts is not a number.
CREATE OR REPLACE TABLE content1 AS
SELECT x.* EXCLUDE (hours_raw),
       round(TRY_CAST(printf('%.30f', x.hours_raw) AS DECIMAL(38, 30)), 2) AS duration_hours
FROM (SELECT DISTINCT ON (content_id) content_id, content_name, content_duration, content_sub_type,
             TRY_CAST(substr(content_duration, 1, 2) AS DOUBLE)
           + TRY_CAST(substr(content_duration, 4, 2) AS DOUBLE) / 60
           + TRY_CAST(substr(content_duration, 7, 2) AS DOUBLE) / 3600 AS hours_raw
      FROM content_src ORDER BY content_id, content_name) x;

CREATE OR REPLACE TABLE org1 AS
SELECT DISTINCT ON (mdo_id) mdo_id, mdo_name, ministry_mdo_id, ministry, department_mdo_id, department, Centre_State
FROM org_src ORDER BY mdo_id, mdo_name;

-- ---------------------------------------------------------------------------
-- Stage 6: the final rows, one per user and course
-- ---------------------------------------------------------------------------
COPY (
SELECT
    ud.user_id,
    c.content_id,
    cn.content_name,
    cn.content_duration,
    cn.content_sub_type,
    coalesce(c.org_effective, ud.mdo_id)                                    AS mdo_id,
    c.isapar,
    c.cb_plan_id,
    c.plan_name,
    c.allocated_on,
    oh.mdo_name,
    oh.ministry_mdo_id,
    oh.ministry,
    oh.department_mdo_id,
    oh.department,
    oh.Centre_State,
    c.due_by                                                                AS due_date,
    (c.org_effective IS NULL)                                               AS cca,
    ud.is_on_central_deputation,
    ud.full_name,
    ud.designation,
    ud.is_from_organised_service_of_govt,
    ud.civil_service_type,
    ud.civil_services,
    ud.cadre_batch,
    ud.cadre,
    ud.email,
    ud.phone_number,
    ud."groups",
    ud.roles,
    CASE WHEN ue.enrolled_on IS NOT NULL THEN 1 ELSE 0 END                  AS enrolments,
    CASE WHEN length(ue.certificate_id) != 0 THEN 1 ELSE 0 END              AS completion,
    CASE WHEN asd.uid IS NOT NULL THEN ud.user_id END                       AS assessment_user_id,
    CASE WHEN asd.uid IS NOT NULL THEN c.content_id END                     AS assessment_content_id,
    CASE WHEN ue.user_consumption_status IS NULL THEN 'not enrolled'
         WHEN lower(ue.user_consumption_status) IN ('not-started', 'not-enrolled', 'in-progress') THEN 'in-progress'
         ELSE ue.user_consumption_status END                                AS user_consumption_status,
    ue.content_progress_percentage,
    ue.certificate_id,
    ue.first_certificate_generated_on,
    ue.enrolled_on,
    CAST(CASE WHEN length(ue.certificate_id) != 0 THEN cn.duration_hours ELSE 0 END AS DECIMAL(12, 2))
                                                                            AS learning_hours,
    CASE WHEN TRY_CAST(asd.score_achieved AS DOUBLE) >= TRY_CAST(asd.cut_off_percentage AS DOUBLE)
          AND asd.assessment_sub_type IN ('Course Assessment', 'Comprehensive Assessment Program')
         THEN 1 ELSE 0 END                                                  AS passed_users,
    CASE WHEN asd.uid IS NOT NULL
          AND asd.assessment_sub_type IN ('Course Assessment', 'Comprehensive Assessment Program')
         THEN 1 ELSE 0 END                                                  AS total_attempts
FROM ranked r
JOIN cp_rs c          ON c.prio = r.prio
JOIN ud               ON ud.uid = r.uid
LEFT JOIN ue1 ue      ON ue.uid = r.uid AND ue.cid = r.cid
LEFT JOIN org1 oh     ON oh.mdo_id = coalesce(c.org_effective, ud.mdo_id)
LEFT JOIN asd1 asd    ON asd.uid = r.uid AND asd.cid = r.cid
LEFT JOIN content1 cn ON cn.content_id = c.content_id
) TO '@@W@@/myigot_new' (FORMAT PARQUET, COMPRESSION 'snappy', FILE_SIZE_BYTES '1GB', OVERWRITE_OR_IGNORE true);
"""

WORK_DB = TMP + "/myigot_work.duckdb"


def log(msg):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  {msg}", flush=True)


def has_parquet(folder):
    return os.path.isdir(folder) and any(f.endswith(".parquet") for f in os.listdir(folder))


def remove(path):
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)
    elif os.path.exists(path):
        os.remove(path)


def cleanup_work():
    for f in (WORK_DB, WORK_DB + ".wal"):
        remove(f)


def fail(msg):
    log(f"ERROR: {msg}")
    log("existing myigot left unchanged")
    remove(OUTPUT + "_new")
    sys.exit(1)


def folder_size(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


class Heartbeat(threading.Thread):
    """Prints a status line while a long stage runs, so a slow stage does not look like a hang."""

    def __init__(self):
        super().__init__(daemon=True)
        self.stage, self.t0 = None, None
        self._stop_event = threading.Event()

    def run(self):
        while not self._stop_event.wait(HEARTBEAT_SECONDS):
            if self.stage:
                log(f"   ... {self.stage} still running ({time.time() - self.t0:,.0f} s) | "
                    f"work files {folder_size(TMP) / 1e9:,.1f} GB | myigot_new {folder_size(OUTPUT + '_new') / 1e9:,.1f} GB")

    def stop(self):
        self._stop_event.set()


def date_expr(dtype, col):
    """SQL that turns a date column into a UTC timestamp (text, timestamps, or nanoseconds since 1970)."""
    if dtype.startswith(("BIGINT", "HUGEINT", "UBIGINT")):
        return f"make_timestamp({col} // 1000)"
    return f"TRY_CAST(TRY_CAST({col} AS TIMESTAMPTZ) AS TIMESTAMP)"


def main():
    start = time.time()
    log("myigot build started")

    lock = open("/tmp/build_myigot_duckdb.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fail("another run is already in progress")

    os.makedirs(TMP, exist_ok=True)
    cleanup_work()
    remove(OUTPUT + "_new")
    atexit.register(cleanup_work)
    con = duckdb.connect(WORK_DB)       # disk-backed: intermediate tables can leave memory
    con.execute("SET TimeZone = 'UTC'")

    # 1. inputs: folders, columns, rows
    for folder, needed in INPUTS.items():
        path = f"{WAREHOUSE}/{folder}"
        if not has_parquet(path):
            fail(f"no parquet files in {path}")
        have = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}/*.parquet')").fetchall()}
        missing = [c for c in needed if c not in have]
        if missing:
            fail(f"{path} is missing columns: {missing}")
        n = con.execute(f"SELECT count(*) FROM read_parquet('{path}/*.parquet')").fetchone()[0]
        if n == 0:
            fail(f"{path} has 0 rows")
        log(f"input {folder:<20} {n:>14,} rows")

    # 2. date columns: how each is stored, and how much text cannot be read
    placeholders = {}
    for key, (folder, col) in DATE_COLUMNS.items():
        path = f"{WAREHOUSE}/{folder}"
        dtype = con.execute(f"DESCRIBE SELECT {col} FROM read_parquet('{path}/*.parquet')").fetchone()[1]
        expr = date_expr(dtype, col)
        placeholders[key] = expr
        note = ""
        if dtype == "VARCHAR":
            nonempty, bad = con.execute(
                f"SELECT count(*) FILTER (WHERE trim({col}) != ''), "
                f"count(*) FILTER (WHERE trim({col}) != '' AND {expr} IS NULL) "
                f"FROM read_parquet('{path}/*.parquet')").fetchone()
            pct = 100.0 * bad / nonempty if nonempty else 0.0
            note = f"   unreadable: {bad:,} of {nonempty:,} ({pct:.3f}%)"
            if pct > MAX_BAD_DATE_PCT:
                fail(f"{folder}.{col}: {pct:.2f}% of the date text cannot be read (limit {MAX_BAD_DATE_PCT}%)")
        elif dtype.startswith(("BIGINT", "HUGEINT", "UBIGINT")):
            note = "   (nanoseconds since 1970: converted)"
        log(f"date  {folder}.{col:<32} {dtype}{note}")

    # 3. the plans and users that feed the build (compare with BigQuery's cb_plan / user_detail)
    plan_path = f"read_parquet('{WAREHOUSE}/cb_plan/*.parquet')"
    live_rows, live_plans, glob_rows, glob_plans, latest = con.execute(
        f"SELECT count(*), count(DISTINCT cb_plan_id), count(*) FILTER (WHERE is_global), "
        f"count(DISTINCT cb_plan_id) FILTER (WHERE is_global), max({placeholders['TS_PLAN_ALLOC']}) "
        f"FROM (SELECT cb_plan_id, allocated_on, (org_id IS NULL OR trim(org_id) = '' OR strpos(org_id, ',') > 0) AS is_global "
        f"FROM {plan_path} WHERE status = 'Live')").fetchone()
    if live_rows == 0:
        fail("cb_plan has no plan with status 'Live'")
    users_active, users_rows = con.execute(
        f"SELECT count(DISTINCT user_id), count(*) FROM read_parquet('{WAREHOUSE}/user_detail/*.parquet') "
        "WHERE TRY_CAST(status AS INTEGER) = 1").fetchone()
    log(f"plans (status Live): {live_rows:,} rows, {live_plans:,} distinct plans; global: {glob_rows:,} rows, "
        f"{glob_plans:,} plans; latest allocated_on {latest}")
    log(f"active users: {users_active:,} (user_detail rows with status 1: {users_rows:,})")

    # 4. build, one statement at a time
    sql = SQL
    for key, expr in placeholders.items():
        sql = sql.replace(f"@@{key}@@", expr)
    sql = (sql.replace("@@W@@", WAREHOUSE).replace("@@MEM@@", MEM)
              .replace("@@THREADS@@", str(THREADS)).replace("@@TMP@@", TMP))
    left = sorted(set(re.findall(r"@@\w+@@", sql)))
    if left:
        fail(f"unfilled placeholders in the SQL: {left}")

    statements = [st for st in re.split(r";[ \t]*\n", re.sub(r"--[^\n]*", "", sql)) if st.strip()]
    log(f"building (memory {MEM}, threads {THREADS})")
    heartbeat = Heartbeat()
    heartbeat.start()
    for st in statements:
        head = next(line for line in st.splitlines() if line.strip()).strip()
        label = None
        if head.startswith("CREATE OR REPLACE TABLE"):
            label = "table " + head.split()[4]
        elif head.startswith("COPY"):
            label = "write myigot"
        elif head.startswith("DROP TABLE"):
            label = "free " + head.split()[2]
        t0 = time.time()
        if label and not label.startswith("free "):
            log(f"   starting {label} ...")
        heartbeat.stage, heartbeat.t0 = label, t0
        try:
            con.execute(st)
        except Exception as e:  # noqa: BLE001
            fail(f"stage '{label or head[:50]}' failed: {e}")
        heartbeat.stage = None
        if label:
            extra = ""
            if label.startswith("table "):
                extra = f"{con.execute(f'SELECT count(*) FROM {head.split()[4]}').fetchone()[0]:>14,} rows"
            log(f"   {label:<22} {time.time() - t0:8.1f}s  {extra}")
        if label == "table allowed":
            keys = con.execute("SELECT rule_key, count(DISTINCT rs_id) FROM allowed GROUP BY 1 ORDER BY 2 DESC").fetchall()
            log("   rule keys used by Live plans: " + ", ".join(f"{k or '?'} ({n:,})" for k, n in keys))
            unknown = [k for k, _ in keys if k not in SUPPORTED_RULE_KEYS]
            if unknown:
                log(f"   note: no user attribute exists for the rule key(s) {unknown}; plans with them reach nobody (as in BigQuery)")

    heartbeat.stop()

    # 5. checks
    new = f"read_parquet('{OUTPUT}_new/*.parquet')"
    q = lambda sql_: con.execute(sql_).fetchone()  # noqa: E731
    rows, pairs, users, plans = q(f"SELECT count(*), count(DISTINCT (user_id, content_id)), "
                                  f"count(DISTINCT user_id), count(DISTINCT cb_plan_id) FROM {new}")
    if rows == 0:
        fail("result has 0 rows")
    if rows != pairs:
        fail(f"{rows - pairs:,} duplicate (user, course) rows")
    if users > users_active:
        fail(f"result has {users:,} users but only {users_active:,} users are active")
    if plans > live_plans:
        fail(f"result has {plans:,} plans but only {live_plans:,} plans are Live")
    log(f"result: {rows:,} rows")
    totals = q(f"SELECT count(DISTINCT user_id), count(DISTINCT mdo_id), count(DISTINCT cb_plan_id), "
               f"count(*) FILTER (WHERE cca), sum(enrolments), sum(completion), round(sum(learning_hours), 2), "
               f"sum(passed_users), sum(total_attempts) FROM {new}")
    for name, v in zip(["distinct_users", "distinct_mdo_id", "distinct_plans", "global_plan_rows (cca)",
                        "sum_enrolments", "sum_completion", "sum_learning_hours", "sum_passed_users",
                        "sum_total_attempts"], totals):
        log(f"   {name:<24}{v!s:>18}")

    if has_parquet(OUTPUT):
        prev = q(f"SELECT count(*) FROM read_parquet('{OUTPUT}/*.parquet')")[0]
        if prev and rows < prev * (1 - MAX_DROP_PCT / 100):
            fail(f"result has {rows:,} rows, the current output has {prev:,} (more than {MAX_DROP_PCT}% fewer)")
        log(f"   current output: {prev:,} rows")
    else:
        log("   no current output to compare with")
    con.close()

    # 6. replace the output
    remove(OUTPUT)
    os.rename(OUTPUT + "_new", OUTPUT)
    files = [f for f in os.listdir(OUTPUT) if f.endswith(".parquet")]
    size = sum(os.path.getsize(f"{OUTPUT}/{f}") for f in files) / 1e9
    log(f"written {OUTPUT}  ({len(files)} files, {size:.1f} GB)")
    log(f"finished in {(time.time() - start) / 60:.1f} min")


if __name__ == "__main__":
    main()
