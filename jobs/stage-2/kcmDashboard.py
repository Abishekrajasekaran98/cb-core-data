#!/usr/bin/env python3
"""
build_kcm.py -- builds warehouse/kcm from kcm_content_mapping, kcm_dictionary and content
with DuckDB. Same logic as BigQuery's view kb_prod_dataset.kcm.

Run:    python3 build_kcm.py
Needs:  pip install duckdb
Exit code 0 = success; anything else = failed, existing output left unchanged.

Safeguards: one run at a time; input files and columns checked; result checked
(rows > 0, not much smaller than the current output); written to kcm_new first and
only then replaces kcm.

Notes:
  * STRING_AGG(DISTINCT x, ', ') in BigQuery has no fixed order; here the values are
    sorted, so every run gives the same text.
  * A course with no competency names gets NULL in the all_competency_* columns, as in
    BigQuery (the Spark job wrote an empty string).
  * Like the BigQuery view, one row per (mapping row x dictionary match): duplicate rows
    in the mapping or dictionary give duplicate rows here too.
"""
import fcntl
import os
import shutil
import sys
import time

import duckdb

# ---------------- settings ----------------
WAREHOUSE    = "/home/analytics/pyspark/warehouse"
OUTPUT       = WAREHOUSE + "/kcm"
MAX_DROP_PCT = 5          # refuse to replace the output if it has this % fewer rows than before
THREADS      = 4
MEM          = "8GB"
# ------------------------------------------

INPUTS = {   # folder -> columns that must exist
    "kcm_content_mapping": ["course_id", "competency_area_id", "competency_theme_id", "competency_sub_theme_id"],
    "kcm_dictionary": ["competency_area_id", "competency_theme_id", "competency_sub_theme_id",
                       "competency_area", "competency_theme", "competency_sub_theme"],
    "content": ["content_id", "content_name", "content_duration", "content_provider_name",
                "content_rating", "content_sub_type", "content_status"],
}

SQL = f"""
CREATE OR REPLACE TABLE result AS
WITH
kcm AS (SELECT * FROM read_parquet('{WAREHOUSE}/kcm_content_mapping/*.parquet')),
kd  AS (SELECT * FROM read_parquet('{WAREHOUSE}/kcm_dictionary/*.parquet')),
c   AS (SELECT * FROM read_parquet('{WAREHOUSE}/content/*.parquet')),

-- all competency names of a course, as sorted distinct text (NULL when there are none)
area_agg AS (
    SELECT
        kcm.course_id,
        string_agg(DISTINCT kd.competency_area,      ', ' ORDER BY kd.competency_area)      AS all_competency_areas,
        string_agg(DISTINCT kd.competency_theme,     ', ' ORDER BY kd.competency_theme)     AS all_competency_theme,
        string_agg(DISTINCT kd.competency_sub_theme, ', ' ORDER BY kd.competency_sub_theme) AS all_competency_sub_themes
    FROM kcm
    LEFT JOIN kd
           ON kcm.competency_area_id      = kd.competency_area_id
          AND kcm.competency_theme_id     = kd.competency_theme_id
          AND kcm.competency_sub_theme_id = kd.competency_sub_theme_id
    GROUP BY kcm.course_id
)

SELECT
    kcm.course_id,
    c.content_name,
    c.content_duration,
    c.content_provider_name,
    c.content_rating,
    c.content_sub_type,
    kcm.competency_area_id,
    kd.competency_area,
    kd.competency_theme,
    kd.competency_sub_theme,
    area_agg.all_competency_areas,
    area_agg.all_competency_theme,
    area_agg.all_competency_sub_themes
FROM kcm
LEFT JOIN c ON kcm.course_id = c.content_id
LEFT JOIN kd
       ON kcm.competency_area_id      = kd.competency_area_id
      AND kcm.competency_theme_id     = kd.competency_theme_id
      AND kcm.competency_sub_theme_id = kd.competency_sub_theme_id
LEFT JOIN area_agg ON kcm.course_id = area_agg.course_id
WHERE c.content_status = 'Live'
  AND c.content_sub_type = 'Course'
  AND kcm.competency_area_id IS NOT NULL
"""


def log(msg):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  {msg}", flush=True)


def fail(msg):
    log(f"ERROR: {msg}")
    log("existing kcm left unchanged")
    sys.exit(1)


def has_parquet(folder):
    return os.path.isdir(folder) and any(f.endswith(".parquet") for f in os.listdir(folder))


def main():
    start = time.time()
    log("kcm build started")

    lock = open("/tmp/build_kcm.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fail("another run is already in progress")

    con = duckdb.connect()
    con.execute(f"SET threads = {THREADS}; SET memory_limit = '{MEM}'")

    # 1. inputs
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
        log(f"input {folder}: {n:,} rows")

    # 2. build
    con.execute(SQL)

    # 3. result checks
    rows, courses, with_names = con.execute(
        "SELECT count(*), count(DISTINCT course_id), count(all_competency_areas) FROM result").fetchone()
    if rows == 0:
        fail("result has 0 rows")
    if has_parquet(OUTPUT):
        prev = con.execute(f"SELECT count(*) FROM read_parquet('{OUTPUT}/*.parquet')").fetchone()[0]
        if prev and rows < prev * (1 - MAX_DROP_PCT / 100):
            fail(f"result has {rows:,} rows, existing output had {prev:,} (more than {MAX_DROP_PCT}% fewer)")
        log(f"result: {rows:,} rows, {courses:,} courses (existing: {prev:,} rows)")
    else:
        log(f"result: {rows:,} rows, {courses:,} courses (no existing output)")
    log(f"   rows with competency names: {with_names:,}")

    # 4. write to kcm_new, verify, replace
    new = OUTPUT + "_new"
    shutil.rmtree(new, ignore_errors=True)
    os.makedirs(new)
    con.execute(f"COPY result TO '{new}/part-00000.parquet' (FORMAT PARQUET, COMPRESSION 'snappy')")
    written = con.execute(f"SELECT count(*) FROM read_parquet('{new}/*.parquet')").fetchone()[0]
    if written != rows:
        fail(f"wrote {written:,} rows, expected {rows:,}")
    con.close()

    shutil.rmtree(OUTPUT, ignore_errors=True)
    os.rename(new, OUTPUT)
    log(f"written {OUTPUT}")
    log(f"finished in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
