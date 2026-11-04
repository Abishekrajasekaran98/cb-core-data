"""
build_content_custom.py -- builds warehouse/content_custom with DuckDB. Same logic as BigQuery's
CREATE TABLE kb_prod_dataset.content_custom: one row per course/event and organisation that has
enrolments (one row for a course/event nobody enrolled in), with enrolment, assessment and
competency columns.

Run:    python3 build_content_custom.py
Needs:  pip install duckdb
Exit code 0 = success; anything else = failed, existing output left unchanged.

Run AFTER build_user_enrolments_custom.py and build_kcm.py (it reads their outputs).
Also needs the content_health_score folder (as the Spark job did).

Safeguards: one run at a time; input folders, columns and row counts checked; unreadable date
text counted (stops above MAX_BAD_DATE_PCT); result checked (every course/event of the content
and event tables is present, some content has status 'Live', no big drop in rows compared with
the current output); written to content_custom_new first and only then replaces content_custom.

Where this follows BigQuery rather than the Spark job
  * all_competency_* is NULL (not an empty string) for a course without competency names
  * INITCAP and TRIM behave as BigQuery's: INITCAP also starts a new word after
    [](){}/|\\<>!?@"^#$&~_,.:;*%+- and whitespace (so 'not-live' becomes 'Not-Live'; Spark would
    give 'Not-live'); TRIM removes all Unicode whitespace (tabs, non-breaking spaces), not only spaces
  * averages are computed in double precision (the Spark job used single precision)
  * last_published_on is a real timestamp (UTC)
Types otherwise as the Spark job (sums are BIGINT; learning_hours is DECIMAL(22,2)).

"The latest assessment" of a course and organisation is the one with the latest completion_date
(rows without a date come last). Exact ties are broken by user id and assessment name, so every
run gives the same result (BigQuery picks an arbitrary one).
"""
import atexit
import fcntl
import os
import re
import shutil
import sys
import time

import duckdb

# ---------------- settings ----------------
WAREHOUSE        = "/home/analytics/pyspark/warehouse"
OUTPUT           = WAREHOUSE + "/content_custom"
MEM              = "64GB"       # keep well below the server RAM
THREADS          = 12
TMP              = WAREHOUSE + "/temp/duckdb_content_custom"   # spill files
MAX_DROP_PCT     = 5            # refuse to replace the output if it has this % fewer rows than the current one
MAX_BAD_DATE_PCT = 0.5          # stop if more than this % of a date column's text cannot be read
# ------------------------------------------

INPUTS = {   # folder -> columns that must exist
    "content": ["content_id", "content_name", "content_duration", "content_type", "content_sub_type",
                "content_provider_name", "content_provider_id", "language", "content_rating", "content_status",
                "scorm_flag", "difficulty_level", "last_published_on"],
    "event_details": ["event_id", "event_name", "duration", "event_type", "event_status",
                      "event_provider_mdo_id", "event_start_datetime"],
    "user_enrolments_custom": ["content_or_event_id", "mdo_id", "mdo_name", "ministry", "department",
                               "ministry_mdo_id", "department_mdo_id", "Centre_State", "enrolments", "completion",
                               "learning_hours", "total_attempts", "passed_users", "user_rating",
                               "number_of_retakes", "time_spent_by_the_user", "score_achieved", "overall_score",
                               "cut_off_percentage", "total_question", "number_of_incorrect_responses",
                               "assessment_user_id", "assessment_content_id", "assessment_name",
                               "assessment_sub_type", "completion_date"],
    "kcm": ["course_id", "all_competency_areas", "all_competency_theme", "all_competency_sub_themes"],
    "content_health_score": ["content_id", "total_health_score"],
}

DATE_COLUMNS = {   # placeholder in the SQL -> (folder, column)
    "TS_CONTENT_PUB": ("content", "last_published_on"),
    "TS_EVENT_START": ("event_details", "event_start_datetime"),
    "TS_UEC_DONE":    ("user_enrolments_custom", "completion_date"),
}

# BigQuery INITCAP: a new word starts after whitespace or one of these characters
INITCAP_DELIMS = '[](){}/|\\<>!?@"^#$&~_,.:;*%+- ' + "\t\n\x0b\x0c\r\xa0"

SQL = r"""
SET memory_limit = '@@MEM@@';
SET threads = @@THREADS@@;
SET temp_directory = '@@TMP@@';
SET preserve_insertion_order = false;
SET TimeZone = 'UTC';

-- BigQuery TRIM: removes all leading and trailing Unicode whitespace
CREATE OR REPLACE MACRO bq_trim(x) AS regexp_replace(x, '^[\s\p{Z}\x0b\x85]+|[\s\p{Z}\x0b\x85]+$', '', 'g');

CREATE OR REPLACE VIEW cnt     AS SELECT * FROM read_parquet('@@W@@/content/*.parquet');
CREATE OR REPLACE VIEW evd     AS SELECT * FROM read_parquet('@@W@@/event_details/*.parquet');
CREATE OR REPLACE VIEW uec_src AS SELECT * FROM read_parquet('@@W@@/user_enrolments_custom/*.parquet');
CREATE OR REPLACE VIEW kcm_src AS SELECT * FROM read_parquet('@@W@@/kcm/*.parquet');
CREATE OR REPLACE VIEW hs_src  AS SELECT content_id AS hs_content_id, total_health_score
                                  FROM read_parquet('@@W@@/content_health_score/*.parquet');

-- BigQuery INITCAP of every distinct status value (first letter of each word upper case, the rest lower case)
CREATE OR REPLACE TABLE initcap_map AS
SELECT raw, string_agg(CASE WHEN i = 1 OR contains('@@DELIMS@@', substr(raw, i - 1, 1))
                            THEN upper(substr(raw, i, 1)) ELSE lower(substr(raw, i, 1)) END, '' ORDER BY i) AS proper
FROM (SELECT raw, unnest(range(1, length(raw) + 1)) AS i
      FROM (SELECT content_status AS raw FROM cnt UNION SELECT event_status FROM evd) WHERE raw IS NOT NULL)
GROUP BY raw
UNION ALL
SELECT '', '';

-- courses and events (events listed in the excluded ids are left out)
CREATE OR REPLACE TABLE merged_content AS
SELECT cnt.content_id, bq_trim(cnt.content_name) AS content_name, cnt.content_duration, cnt.content_type,
       cnt.content_sub_type, cnt.content_provider_name, cnt.content_provider_id, cnt."language", cnt.content_rating,
       im.proper AS content_status, cnt.scorm_flag, cnt.difficulty_level,
       @@TS_CONTENT_PUB@@ AS last_published_on
FROM cnt
LEFT JOIN initcap_map im ON im.raw = cnt.content_status
UNION ALL BY NAME
SELECT evd.event_id AS content_id, bq_trim(evd.event_name) AS content_name, evd.duration AS content_duration,
       evd.event_type AS content_type, 'Event' AS content_sub_type, evd.event_provider_mdo_id AS content_provider_id,
       im.proper AS content_status, @@TS_EVENT_START@@ AS last_published_on
FROM evd
LEFT JOIN initcap_map im ON im.raw = evd.event_status
WHERE evd.event_id NOT IN ('do_11419375418191052819', 'do_114193445681192960140', 'do_11419347470261452812',
                           'do_11419313252346265611', 'do_114193443002286080139');

-- per course/event and organisation: totals, averages and the latest assessment
-- (rows without a course/event can never match a course, so they are left out)
CREATE OR REPLACE TABLE uec_mdo_content AS
SELECT
    content_or_event_id AS content_id,
    mdo_id,
    any_value(mdo_name)           AS mdo_name,
    any_value(ministry)           AS ministry,
    any_value(department)         AS department,
    any_value(ministry_mdo_id)    AS ministry_mdo_id,
    any_value(department_mdo_id)  AS department_mdo_id,
    any_value(Centre_State)       AS Centre_State,
    CAST(sum(enrolments)                 AS BIGINT)        AS enrolments,
    CAST(sum(completion)                 AS BIGINT)        AS completion,
    CAST(sum(learning_hours)             AS DECIMAL(22, 2)) AS learning_hours,
    CAST(sum(total_attempts)             AS BIGINT)        AS total_attempts,
    CAST(sum(passed_users)               AS BIGINT)        AS passed_users,
    avg(TRY_CAST(user_rating            AS DOUBLE))        AS avg_user_rating,
    avg(TRY_CAST(number_of_retakes      AS DOUBLE))        AS avg_retakes_per_user,
    avg(TRY_CAST(time_spent_by_the_user AS DOUBLE))        AS time_spent_by_the_user,
    avg(TRY_CAST(score_achieved         AS DOUBLE))        AS score_achieved,
    avg(TRY_CAST(overall_score          AS DOUBLE))        AS overall_score,
    avg(TRY_CAST(cut_off_percentage     AS DOUBLE))        AS cut_off_percentage,
    CAST(sum(TRY_CAST(total_question AS BIGINT)) AS BIGINT)                AS total_question,
    CAST(sum(TRY_CAST(number_of_incorrect_responses AS BIGINT)) AS BIGINT) AS number_of_incorrect_responses,
    -- the assessment with the latest completion_date; a row with a date always beats one without, and
    -- rows with the same date (or no date at all) are told apart by user id and name, so runs are repeatable
    arg_max(struct_pack(assessment_user_id := assessment_user_id, assessment_content_id := assessment_content_id,
                        assessment_name := assessment_name, assessment_sub_type := assessment_sub_type),
            CASE WHEN @@TS_UEC_DONE@@ IS NOT NULL THEN '1' || strftime(@@TS_UEC_DONE@@, '%Y%m%d%H%M%S%f') ELSE '0' END
            || '|' || coalesce(assessment_user_id, '') || '|' || coalesce(assessment_name, '')) AS latest_assessment
FROM uec_src
WHERE content_or_event_id IS NOT NULL
GROUP BY content_or_event_id, mdo_id;

-- competency names of a course as sorted distinct text (NULL when there are none)
CREATE OR REPLACE TABLE kcm_agg AS
SELECT course_id,
       string_agg(DISTINCT all_competency_areas,     ' | ' ORDER BY all_competency_areas)     AS all_competency_areas,
       string_agg(DISTINCT all_competency_theme,     ' | ' ORDER BY all_competency_theme)     AS all_competency_theme,
       string_agg(DISTINCT all_competency_sub_themes, ' | ' ORDER BY all_competency_sub_themes) AS all_competency_sub_themes
FROM kcm_src
GROUP BY course_id;

COPY (
WITH cws AS (
    SELECT mc.*,
           coalesce(TRY_CAST(string_split(mc.content_duration, ':')[1] AS BIGINT), 0) * 3600
         + coalesce(TRY_CAST(string_split(mc.content_duration, ':')[2] AS BIGINT), 0) * 60
         + coalesce(TRY_CAST(string_split(mc.content_duration, ':')[3] AS BIGINT), 0) AS total_secs
    FROM merged_content mc
)
SELECT
    cws.content_id, cws.content_name, cws.content_duration, cws.content_type, cws.content_sub_type,
    cws.content_provider_name, cws.content_provider_id, cws."language", cws.content_rating, cws.content_status,
    cws.scorm_flag, cws.difficulty_level, cws.last_published_on,

    umc.mdo_id, umc.mdo_name, umc.ministry, umc.department, umc.ministry_mdo_id, umc.department_mdo_id,
    umc.Centre_State,

    umc.enrolments, umc.completion, umc.learning_hours,

    umc.latest_assessment.assessment_user_id      AS assessment_user_id,
    umc.latest_assessment.assessment_content_id   AS assessment_content_id,
    CAST(NULL AS VARCHAR)                         AS assessment_id,
    umc.latest_assessment.assessment_name         AS assessment_name,
    umc.latest_assessment.assessment_sub_type     AS assessment_type,

    CAST(round(umc.score_achieved) AS BIGINT)     AS formatted_score_acieved,
    umc.score_achieved, umc.overall_score, umc.cut_off_percentage, umc.total_question,
    umc.number_of_incorrect_responses, umc.time_spent_by_the_user,

    umc.total_attempts, umc.passed_users, umc.avg_retakes_per_user,

    CASE WHEN cws.content_status = 'Live' AND cws.content_sub_type = 'Course' THEN
             CASE WHEN cws.total_secs < 30 * 60     THEN '0 Hrs to 0.5 Hrs'
                  WHEN cws.total_secs < 60 * 60     THEN '0.5 Hrs to 1 Hr'
                  WHEN cws.total_secs < 2 * 60 * 60 THEN '1 Hr to 2 Hrs'
                  WHEN cws.total_secs < 5 * 60 * 60 THEN '2 Hrs to 5 Hrs'
                  ELSE 'Greater than 5 Hrs' END
         ELSE NULL END                            AS duration_bucket,

    CASE WHEN umc.avg_user_rating IS NOT NULL THEN
             CASE WHEN umc.avg_user_rating > 0 AND umc.avg_user_rating < 1 THEN '0 to 1'
                  WHEN umc.avg_user_rating >= 1 AND umc.avg_user_rating < 2 THEN '1 to 2'
                  WHEN umc.avg_user_rating >= 2 AND umc.avg_user_rating < 3 THEN '2 to 3'
                  WHEN umc.avg_user_rating >= 3 AND umc.avg_user_rating < 4 THEN '3 to 4'
                  WHEN umc.avg_user_rating >= 4 AND umc.avg_user_rating < 5 THEN '4 to 5'
                  WHEN umc.avg_user_rating = 5 THEN '5'
                  ELSE 'Not Rated' END
         ELSE NULL END                            AS rating_bucket,

    CASE WHEN coalesce(umc.total_question, 0) > 10 THEN 'More than 10'
         WHEN coalesce(umc.total_question, 0) = 10 THEN 'Equal to 10'
         WHEN coalesce(umc.total_question, 0) > 0  THEN 'Less than 10'
         ELSE 'Unknown' END                       AS question_count_bucket,

    kcm.all_competency_areas, kcm.all_competency_theme, kcm.all_competency_sub_themes,

    hs.total_health_score
FROM cws
LEFT JOIN uec_mdo_content umc ON umc.content_id = cws.content_id
LEFT JOIN kcm_agg kcm         ON kcm.course_id  = cws.content_id
LEFT JOIN hs_src hs           ON hs.hs_content_id = cws.content_id
) TO '@@W@@/content_custom_new' (FORMAT PARQUET, COMPRESSION 'snappy', FILE_SIZE_BYTES '1GB', OVERWRITE_OR_IGNORE true);
"""

WORK_DB = TMP + "/content_custom_work.duckdb"


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
    log("existing content_custom left unchanged")
    remove(OUTPUT + "_new")
    sys.exit(1)


def date_expr(dtype, col):
    """SQL that turns a date column into a UTC timestamp (text, timestamps, or nanoseconds since 1970)."""
    if dtype.startswith(("BIGINT", "HUGEINT", "UBIGINT")):
        return f"make_timestamp({col} // 1000)"
    return f"TRY_CAST(TRY_CAST({col} AS TIMESTAMPTZ) AS TIMESTAMP)"


def main():
    start = time.time()
    log("content_custom build started")

    lock = open("/tmp/build_content_custom.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fail("another run is already in progress")

    os.makedirs(TMP, exist_ok=True)
    cleanup_work()
    remove(OUTPUT + "_new")
    atexit.register(cleanup_work)
    con = duckdb.connect(WORK_DB)
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
        log(f"input {folder:<24} {n:>14,} rows")

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
        log(f"date  {folder}.{col:<26} {dtype}{note}")

    # 3. build, one statement at a time
    sql = SQL
    for key, expr in placeholders.items():
        sql = sql.replace(f"@@{key}@@", expr)
    sql = (sql.replace("@@DELIMS@@", INITCAP_DELIMS.replace("'", "''"))
              .replace("@@W@@", WAREHOUSE).replace("@@MEM@@", MEM)
              .replace("@@THREADS@@", str(THREADS)).replace("@@TMP@@", TMP))
    left = sorted(set(re.findall(r"@@\w+@@", sql)))
    if left:
        fail(f"unfilled placeholders in the SQL: {left}")

    statements = [st for st in re.split(r";[ \t]*\n", re.sub(r"--[^\n]*", "", sql)) if st.strip()]
    log(f"building (memory {MEM}, threads {THREADS})")
    for st in statements:
        head = next(line for line in st.splitlines() if line.strip()).strip()
        label = None
        if head.startswith("CREATE OR REPLACE TABLE"):
            label = "table " + head.split()[4]
        elif head.startswith("COPY"):
            label = "write content_custom"
        t0 = time.time()
        try:
            con.execute(st)
        except Exception as e:  # noqa: BLE001
            fail(f"stage '{label or head[:50]}' failed: {e}")
        if label:
            extra = ""
            if label.startswith("table "):
                extra = f"{con.execute(f'SELECT count(*) FROM {head.split()[4]}').fetchone()[0]:>14,} rows"
            log(f"   {label:<30} {time.time() - t0:8.1f}s  {extra}")

    # 4. checks
    new = f"read_parquet('{OUTPUT}_new/*.parquet')"
    q = lambda sql_: con.execute(sql_).fetchone()  # noqa: E731
    rows, ids, live = q(f"SELECT count(*), count(DISTINCT content_id), "
                        f"count(*) FILTER (WHERE content_status = 'Live') FROM {new}")
    merged_rows, merged_ids = q("SELECT count(*), count(DISTINCT content_id) FROM merged_content")
    if rows == 0:
        fail("result has 0 rows")
    if ids != merged_ids:
        fail(f"{merged_ids - ids:,} courses/events of the content and event tables are missing from the result")
    if rows < merged_rows:
        fail(f"result has {rows:,} rows, fewer than the {merged_rows:,} courses/events")
    if live == 0:
        fail("no content has the status 'Live' (check the status values in the content table)")

    log("content status values (after INITCAP):")
    for status, n in con.execute("SELECT content_status, count(*) FROM merged_content "
                                 "GROUP BY 1 ORDER BY 2 DESC LIMIT 8").fetchall():
        log(f"   {n:>10,}  {status!r}")
    dups = q(f"SELECT count(*) FROM (SELECT content_id, mdo_id FROM {new} GROUP BY content_id, mdo_id HAVING count(*) > 1)")[0]
    if dups:
        log(f"note: {dups:,} (content_id, mdo_id) pairs appear more than once "
            "(duplicate ids in content/event_details/content_health_score, as in BigQuery)")
    log(f"result: {rows:,} rows for {ids:,} courses/events")
    for name, expr in [("rows with enrolments", "count(enrolments)"),
                       ("rows with competencies", "count(all_competency_areas)"),
                       ("rows with health score", "count(total_health_score)"),
                       ("sum enrolments", "sum(enrolments)"), ("sum completion", "sum(completion)"),
                       ("sum learning_hours", "round(sum(learning_hours), 2)")]:
        log(f"   {name:<26}{q(f'SELECT {expr} FROM {new}')[0]!s:>18}")

    if has_parquet(OUTPUT):
        prev = q(f"SELECT count(*) FROM read_parquet('{OUTPUT}/*.parquet')")[0]
        if prev and rows < prev * (1 - MAX_DROP_PCT / 100):
            fail(f"result has {rows:,} rows, the current output has {prev:,} (more than {MAX_DROP_PCT}% fewer)")
        log(f"   current output: {prev:,} rows")
    else:
        log("   no current output to compare with")
    con.close()

    # 5. replace the output
    remove(OUTPUT)
    os.rename(OUTPUT + "_new", OUTPUT)
    files = [f for f in os.listdir(OUTPUT) if f.endswith(".parquet")]
    size = sum(os.path.getsize(f"{OUTPUT}/{f}") for f in files) / 1e9
    log(f"written {OUTPUT}  ({len(files)} files, {size:.1f} GB)")
    log(f"finished in {(time.time() - start) / 60:.1f} min")


if __name__ == "__main__":
    main()
