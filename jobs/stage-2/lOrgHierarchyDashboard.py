#!/usr/bin/env python3
"""
build_l_org_hierarchy.py -- builds warehouse/L_org_hierarchy from warehouse/org_hierarchy_new
with DuckDB. Same logic as BigQuery's CREATE TABLE kb_prod_dataset.L_org_hierarchy.

Run:    python3 build_l_org_hierarchy.py
Needs:  pip install duckdb
Exit code 0 = success; anything else = failed, previous output left unchanged.

Safeguards:
  * only one run at a time (lock file)
  * checks the input has the expected columns and rows
  * checks the result before using it: one row per mdo_id, no empty mdo_id,
    Centre_State only State / Centre / Others, row count not suddenly much lower
  * writes to L_org_hierarchy_new first, and only replaces L_org_hierarchy once the
    new version is complete and checked

Differences from BigQuery (deliberate, results otherwise identical):
  * where BigQuery keeps an arbitrary row among exact ties, this keeps a fixed one
    (extra tie-breakers: ministry, department, mdo_name), so every run is repeatable
"""
import fcntl
import os
import shutil
import sys
import time

import duckdb

# ---------------- settings ----------------
WAREHOUSE    = "/home/analytics/pyspark/warehouse"
SOURCE       = WAREHOUSE + "/org_hierarchy_new"
OUTPUT       = WAREHOUSE + "/L_org_hierarchy"
MAX_DROP_PCT = 5          # refuse to replace the output if it has this % fewer rows than before
THREADS      = 4          # small table: a few cores are plenty
MEM          = "8GB"
# ------------------------------------------

STATE_NAMES = [
    'ANDAMAN AND NICOBAR', 'ANDHRA PRADESH', 'ARUNACHAL PRADESH', 'ASSAM', 'BIHAR',
    'CHANDIGARH', 'CHHATTISGARH', 'DADRA AND NAGAR HAVELI AND DAMAN AND DIU',
    'DELHI', 'GOA', 'GUJARAT', 'HARYANA', 'HIMACHAL PRADESH', 'JAMMU AND KASHMIR',
    'JHARKHAND', 'KARNATAKA', 'KERALA', 'LADAKH', 'LAKSHADWEEP', 'MADHYA PRADESH',
    'MAHARASHTRA', 'MANIPUR', 'MEGHALAYA', 'MIZORAM', 'NAGALAND', 'ODISHA',
    'PUDUCHERRY', 'PUNJAB', 'RAJASTHAN', 'SIKKIM', 'TAMIL NADU', 'TELANGANA',
    'TRIPURA', 'UTTAR PRADESH', 'UTTARAKHAND', 'WEST BENGAL',
]

# full list from the BigQuery query
CENTRE_IDS = [
    '0135502408754298881027', '0137031449230295041079', '013697475704823808224',
    '0135502384753049601025', '01351476852294451276', '0135369191364116480',
    '0138425609370419204618', '0135502379180195841022', '0135502311333232641021',
    '0135502372833034241020', '0135502307998433281018', '0135502333539614721017',
    '0134822680586076163541', '0135536923694202881', '0135502299683143681014',
    '0135502300487188481012', '0135502312177008641011', '0135502348119818241010',
    '013619587009298432517', '0135502275499212801008', '0135502335186124801006',
    '0135536950938992640', '0134913087249776646170', '0135502292299776001004',
    '0135502316148080641003', '0135502270820679681002', '0135502308469719041001',
    '0135502275984916481000', '0135536987143127041', '01341208111149875242',
    '01373417051866726412130', '014371835358019584946', '01341209094833766448',
    '013550226147295232996', '013550223392186368995', '013550223656493056994',
    '013550227183403008993', '013550227710910464992', '013550224741203968991',
    '013550220256387072989', '013690887239426048425', '0135536967639613440',
    '0136060787211714564847', '01375416142607974414755', '013759612859408384146',
    '0135807545852395523', '01341289657531596898', '0135502356495564801024',
    '013417171898122240115', '0135346646711500804337', '0135536978418237440',
    '013422057934061568836', '013422003222487040830', '0134447739702476803',
    '0135536923582873602', '01346691669720268818', '01358290535835238436',
    '013623128609079296444', '013417196267282432142', '013421994704461824827',
    '0133872821604761600', '0137089219607265282903', '01383606259748044812174',
    '01341081529829785612', '013550228078960640998', '0135042365743759366378',
    '013550224835502080990', '0135502367397068801019', '013550228160208896999',
    '0136727760176332801621', '0135502343884800001009', '0135502319381626881023',
    '0135502344140963841016', '0135536918196060163', '01369536911994060842',
    '0135502272929464321007', '013550225227939840997', '0135502283206246401005',
    '013616064336633856160', '0135502334220861441013', '0137668882295275526295',
]

REQUIRED_COLUMNS = ["center_state_id", "center_state_name"] + \
                   [f"l{i}_{x}" for i in range(1, 11) for x in ("id", "name")]


def log(msg):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  {msg}", flush=True)


def fail(msg):
    log(f"ERROR: {msg}")
    log("previous L_org_hierarchy left unchanged")
    sys.exit(1)


def sql_list(values):
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def build_sql():
    # one row per hierarchy level (BigQuery's UNNEST of 11 STRUCTs)
    levels = [
        "SELECT CAST(center_state_id AS VARCHAR) AS mdo_id, CAST(center_state_name AS VARCHAR) AS mdo_name, "
        "NULL::VARCHAR AS ministry_id, NULL::VARCHAR AS ministry, "
        "NULL::VARCHAR AS department_id, NULL::VARCHAR AS department FROM src",
        "SELECT CAST(l1_id AS VARCHAR), CAST(l1_name AS VARCHAR), "
        "CAST(center_state_id AS VARCHAR), CAST(center_state_name AS VARCHAR), "
        "NULL::VARCHAR, NULL::VARCHAR FROM src",
    ] + [
        f"SELECT CAST(l{i}_id AS VARCHAR), CAST(l{i}_name AS VARCHAR), "
        "CAST(center_state_id AS VARCHAR), CAST(center_state_name AS VARCHAR), "
        "CAST(l1_id AS VARCHAR), CAST(l1_name AS VARCHAR) FROM src"
        for i in range(2, 11)
    ]
    return f"""
CREATE OR REPLACE TABLE result AS
WITH
src AS (SELECT * FROM read_parquet('{SOURCE}/*.parquet')),

hierarchy_with_ids AS (
    SELECT DISTINCT * FROM (
        {chr(10).join(('        UNION ALL ' if n else '        ') + q for n, q in enumerate(levels))}
    )
    WHERE mdo_id IS NOT NULL AND mdo_id != ''
),

base_raw AS (
    SELECT
        b.mdo_id,
        b.mdo_name,
        CASE WHEN b.ministry_id   IS NOT NULL AND b.ministry_id   != '' THEN ml_by_id.mdo_name
             WHEN b.ministry      IS NOT NULL AND b.ministry      != '' THEN ml_by_name.mdo_name
             WHEN b.department_id IS NOT NULL AND b.department_id != '' THEN dl_by_id.mdo_name
             WHEN b.department    IS NOT NULL AND b.department    != '' THEN dl_by_name.mdo_name
             ELSE b.mdo_name END                                              AS ministry,
        CASE WHEN b.ministry_id   IS NOT NULL AND b.ministry_id   != '' THEN b.ministry_id
             WHEN b.ministry      IS NOT NULL AND b.ministry      != '' THEN ml_by_name.mdo_id
             WHEN b.department_id IS NOT NULL AND b.department_id != '' THEN b.department_id
             WHEN b.department    IS NOT NULL AND b.department    != '' THEN dl_by_name.mdo_id
             ELSE b.mdo_id END                                                AS ministry_mdo_id,
        coalesce(dl_by_id.mdo_name, dl_by_name.mdo_name, b.mdo_name)          AS department,
        coalesce(b.department_id,   dl_by_name.mdo_id,   b.mdo_id)            AS department_mdo_id
    FROM hierarchy_with_ids b
    LEFT JOIN hierarchy_with_ids ml_by_id   ON ml_by_id.mdo_id     = b.ministry_id
    LEFT JOIN hierarchy_with_ids ml_by_name ON ml_by_name.mdo_name = b.ministry
    LEFT JOIN hierarchy_with_ids dl_by_id   ON dl_by_id.mdo_id     = b.department_id
    LEFT JOIN hierarchy_with_ids dl_by_name ON dl_by_name.mdo_name = b.department
),

-- one row per mdo_id; BigQuery sorts NULLs first in ascending order, so do the same
base AS (
    SELECT * FROM base_raw
    QUALIFY row_number() OVER (
        PARTITION BY mdo_id
        ORDER BY ministry_mdo_id ASC NULLS FIRST, department_mdo_id ASC NULLS FIRST,
                 ministry ASC NULLS FIRST, department ASC NULLS FIRST, mdo_name ASC NULLS FIRST
    ) = 1
)

SELECT
    mdo_id, mdo_name, ministry, ministry_mdo_id, department, department_mdo_id,
    CASE WHEN upper(trim(ministry)) IN ({sql_list(STATE_NAMES)}) THEN 'State'
         WHEN CAST(ministry_mdo_id AS VARCHAR) IN ({sql_list(CENTRE_IDS)}) THEN 'Centre'
         ELSE 'Others' END                                                    AS Centre_State
FROM base
"""


def main():
    start = time.time()
    log("L_org_hierarchy build started")

    # one run at a time
    lock = open("/tmp/build_l_org_hierarchy.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fail("another run is already in progress")

    con = duckdb.connect()
    con.execute(f"SET threads = {THREADS}; SET memory_limit = '{MEM}'")

    # 1. input checks
    if not os.path.isdir(SOURCE) or not any(f.endswith(".parquet") for f in os.listdir(SOURCE)):
        fail(f"no parquet files in {SOURCE}")
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{SOURCE}/*.parquet')").fetchall()}
    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        fail(f"{SOURCE} is missing columns: {missing}")
    src_rows = con.execute(f"SELECT count(*) FROM read_parquet('{SOURCE}/*.parquet')").fetchone()[0]
    if src_rows == 0:
        fail(f"{SOURCE} has 0 rows")
    log(f"input org_hierarchy_new: {src_rows:,} rows")

    # 2. build
    con.execute(build_sql())

    # 3. result checks
    rows, ids, empty_ids = con.execute(
        "SELECT count(*), count(DISTINCT mdo_id), count(*) FILTER (WHERE mdo_id IS NULL OR mdo_id = '') FROM result"
    ).fetchone()
    if rows == 0:
        fail("result has 0 rows")
    if ids != rows:
        fail(f"{rows - ids} duplicate mdo_id rows")
    if empty_ids:
        fail(f"{empty_ids} rows without mdo_id")
    bad_cs = con.execute("SELECT count(*) FROM result WHERE Centre_State NOT IN ('State', 'Centre', 'Others')").fetchone()[0]
    if bad_cs:
        fail(f"{bad_cs} rows with an unexpected Centre_State")

    if os.path.isdir(OUTPUT) and any(f.endswith(".parquet") for f in os.listdir(OUTPUT)):
        prev = con.execute(f"SELECT count(*) FROM read_parquet('{OUTPUT}/*.parquet')").fetchone()[0]
        if prev and rows < prev * (1 - MAX_DROP_PCT / 100):
            fail(f"result has {rows:,} rows, previous had {prev:,} (more than {MAX_DROP_PCT}% fewer)")
        log(f"result: {rows:,} rows (previous: {prev:,})")
    else:
        log(f"result: {rows:,} rows (no previous output)")
    for cs, n in con.execute("SELECT Centre_State, count(*) FROM result GROUP BY 1 ORDER BY 1").fetchall():
        log(f"   {cs:<8} {n:>8,}")

    # 4. write to _new, verify, then replace the existing output
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
