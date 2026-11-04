#!/usr/bin/env python3
"""
build_user_enrolments_custom.py -- builds, with DuckDB, the MDO dashboard tables:

    user_enrolments_custom   same logic as BigQuery's kb_prod_dataset.user_enrolments_custom
    mdo_enrolment_fact       1 row per user_enrolments_custom row   -> warehouse.mdo_enrolment_fact
    mdo_user_summary         1 row per user                         -> warehouse.mdo_user_summary
    mdo_user_access          from user_detail                       -> warehouse.user_access
    mdo_org_access           from L_org_hierarchy                   -> warehouse.org_access

Run:    python3 build_user_enrolments_custom.py
Needs:  pip install duckdb
Exit code 0 = success; anything else = failed, existing outputs left unchanged.

Run AFTER build_l_org_hierarchy.py and build_national_priority_mapping_duckdb.py (it reads them).

Safeguards
  * one run at a time
  * inputs: folders, columns and row counts checked; date text that cannot be read is counted
    and the run stops if too much of it is unreadable
  * results checked: every active user is present, one row per user in mdo_user_summary,
    table sizes agree with each other, no big drop in rows compared with the current outputs
  * all five outputs are built into <name>_new folders; only when every check passes do they
    replace the current ones

Where this follows BigQuery rather than the Spark job
  * registration_method: BigQuery's patterns '.in$', '.org$', '.pfcindia.com$' have unescaped dots,
    so user@pfcindia.com counts as "Government Email" (the Spark job, with escaped dots, said
    "Personal Email"). Set REGISTRATION_LIKE_BIGQUERY = False to keep the Spark behaviour.
    The script prints how many users are affected.
  * learning_hours: a completed item whose duration has a missing or unreadable part gets NULL (0 in
    totals); the Spark job counted the readable parts instead, so its totals were slightly higher.
    Rounding to 2 decimals follows BigQuery exactly, including exact ties (BigQuery rounds the value the
    double is really stored as; DuckDB's own round() would round some ties up, about +0.003% of hours)
Dates: all date columns are real timestamps (UTC); the Spark job wrote some as text, which
made the ClickHouse load fail on values like '2026-04-01T09:00:00Z'.
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
WAREHOUSE                  = "/home/analytics/pyspark/warehouse"
MEM                        = "120GB"    # keep well below the server RAM (DuckDB uses some extra)
THREADS                    = 12
TMP                        = WAREHOUSE + "/temp/duckdb_uec"   # work database and spill files (needs ~100 GB free)
REGISTRATION_LIKE_BIGQUERY = True       # see the note above
MAX_DROP_PCT               = 5          # refuse to replace an output that has this % fewer rows than the current one
MAX_BAD_DATE_PCT           = 0.5        # stop if more than this % of a date column's text cannot be read
# ------------------------------------------

OUTPUTS = ["user_enrolments_custom", "mdo_enrolment_fact", "mdo_user_summary", "mdo_user_access", "mdo_org_access"]

INPUTS = {   # folder -> columns that must exist
    "national_priority_mapping": ["content_id", "theme"],
    "content": ["content_id", "content_name", "content_duration", "content_type", "content_sub_type",
                "content_provider_name", "content_status"],
    "event_details": ["event_id", "event_name", "duration", "event_type", "event_status"],
    "user_enrolments": ["user_id", "content_id", "enrolled_on", "certificate_id", "badge_id",
                        "first_certificate_generated_on", "user_consumption_status", "live_cbp_plan_mandate",
                        "user_rating", "content_progress_percentage"],
    "event_enrolment_details": ["user_id", "event_id", "enrolled_on_datetime", "certificate_id",
                                "completed_on_datetime", "status", "completion_percentage"],
    "user_detail": ["user_id", "full_name", "designation", "email", "gender", "phone_number", "groups",
                    "user_registration_date", "tag", "mdo_id", "roles", "profile_status", "external_system_id",
                    "external_system", "employee_id", "is_from_organised_service_of_govt", "civil_service_type",
                    "civil_services", "cadre_batch", "cadre", "status"],
    "L_org_hierarchy": ["mdo_id", "mdo_name", "ministry", "department", "ministry_mdo_id",
                        "department_mdo_id", "Centre_State"],
    "assessment_detail": ["user_id", "content_id", "assessment_id", "assessment_name", "assessment_sub_type",
                          "assessment_duration", "time_spent_by_the_user", "pass", "completion_date",
                          "score_achieved", "overall_score", "cut_off_percentage", "total_question",
                          "number_of_incorrect_responses", "number_of_retakes"],
}

DATE_COLUMNS = {   # placeholder in the SQL -> (folder, column)
    "TS_UEN_ENR":   ("user_enrolments", "enrolled_on"),
    "TS_UEN_FIRST": ("user_enrolments", "first_certificate_generated_on"),
    "TS_EED_ENR":   ("event_enrolment_details", "enrolled_on_datetime"),
    "TS_EED_DONE":  ("event_enrolment_details", "completed_on_datetime"),
    "TS_UDT_REG":   ("user_detail", "user_registration_date"),
    "TS_ASM_DONE":  ("assessment_detail", "completion_date"),
}

SQL = r"""
SET memory_limit = '@@MEM@@';
SET threads = @@THREADS@@;
SET temp_directory = '@@TMP@@';
SET preserve_insertion_order = false;
SET TimeZone = 'UTC';

CREATE OR REPLACE VIEW npm_src AS SELECT * FROM read_parquet('@@W@@/national_priority_mapping/*.parquet');
CREATE OR REPLACE VIEW cnt     AS SELECT * FROM read_parquet('@@W@@/content/*.parquet');
CREATE OR REPLACE VIEW evd     AS SELECT * FROM read_parquet('@@W@@/event_details/*.parquet');
CREATE OR REPLACE VIEW uen     AS SELECT * FROM read_parquet('@@W@@/user_enrolments/*.parquet');
CREATE OR REPLACE VIEW eed     AS SELECT * FROM read_parquet('@@W@@/event_enrolment_details/*.parquet');
CREATE OR REPLACE VIEW udt     AS SELECT * FROM read_parquet('@@W@@/user_detail/*.parquet');
CREATE OR REPLACE VIEW ohy     AS SELECT * FROM read_parquet('@@W@@/L_org_hierarchy/*.parquet');
CREATE OR REPLACE VIEW asm     AS SELECT * FROM read_parquet('@@W@@/assessment_detail/*.parquet');

-- ---------------------------------------------------------------------------
-- Stage 1: small lookups
-- ---------------------------------------------------------------------------
-- BigQuery groups by (content_id, theme) while counting distinct themes, so theme_count is always 1;
-- reproduced as it is
CREATE OR REPLACE TABLE theme_rollup AS
SELECT content_id AS tr_content_id, theme AS tr_theme, count(DISTINCT theme) AS tr_theme_count
FROM (SELECT DISTINCT content_id, theme FROM npm_src)
GROUP BY content_id, theme;

-- duration_hours = BigQuery's ROUND(hours + minutes / 60 + seconds / 3600, 2) on FLOAT64, worked out once per
-- course/event. BigQuery rounds the exact value the double is stored as (so 0.085 stored as 0.08499999... gives
-- 0.08); DuckDB's own round() rounds some of those ties up. Printing the double's exact digits and rounding
-- those reproduces BigQuery. NULL when any of the three parts is missing or not a number.
CREATE OR REPLACE TABLE merged_content AS
SELECT m.* EXCLUDE (hours_raw),
       round(TRY_CAST(printf('%.30f', m.hours_raw) AS DECIMAL(38, 30)), 2) AS duration_hours
FROM (
    SELECT *,
           TRY_CAST(string_split(duration, ':')[1] AS DOUBLE)
         + TRY_CAST(string_split(duration, ':')[2] AS DOUBLE) / 60
         + TRY_CAST(string_split(duration, ':')[3] AS DOUBLE) / 3600 AS hours_raw
    FROM (
        SELECT content_id AS id, content_name AS name, content_duration AS duration, content_type AS type,
               content_sub_type AS sub_type, 'Course' AS content_or_event, content_provider_name, content_status
        FROM cnt
        UNION ALL BY NAME
        SELECT event_id AS id, event_name AS name, duration AS duration, event_type AS type,
               'Event' AS sub_type, 'Event' AS content_or_event, NULL::VARCHAR AS content_provider_name,
               event_status AS content_status
        FROM evd
    )
) m;

-- ---------------------------------------------------------------------------
-- Stage 2: active users with their organisation (users without enrolments stay)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE ud2 AS
SELECT
    ud.user_id, ud.full_name, ud.designation, ud.email, ud.gender,
    regexp_replace(ud.email, '(@.*)', '@****')                    AS masked_email,
    regexp_replace(ud.phone_number, '(\d{3})$', '***')            AS masked_phone_number,
    ud.phone_number, ud."groups",
    @@TS_UDT_REG@@                                                AS user_registration_date,
    ud.tag, ud.mdo_id, ud.roles, ud.profile_status, ud.external_system_id, ud.external_system,
    ud.employee_id, ud.is_from_organised_service_of_govt, ud.civil_service_type, ud.civil_services,
    ud.cadre_batch, ud.cadre,
    oh.mdo_name, oh.ministry, oh.department, oh.ministry_mdo_id, oh.department_mdo_id, oh.Centre_State,
    CASE WHEN @@GOV_EXPR@@ THEN 'Government Email' ELSE 'Personal Email' END AS registration_method
FROM udt ud
LEFT JOIN ohy oh ON oh.mdo_id = ud.mdo_id
WHERE TRY_CAST(ud.status AS INTEGER) = 1;

-- ---------------------------------------------------------------------------
-- Stage 3: enrolments joined to content, theme and assessments (one row per enrolment x assessment)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE facts AS
WITH
me AS (
    SELECT user_id, content_id AS id,
           @@TS_UEN_ENR@@                                         AS enrolled_on,
           certificate_id, badge_id,
           @@TS_UEN_FIRST@@                                       AS first_certificate_generated_on,
           coalesce(user_consumption_status, 'Not Active')        AS user_consumption_status,
           @@LIVE_BOOL@@                                          AS live_cbp_plan_mandate,
           TRY_CAST(user_rating AS FLOAT)                         AS user_rating,
           TRY_CAST(content_progress_percentage AS DOUBLE)        AS content_progress_percentage
    FROM uen
    UNION ALL BY NAME
    SELECT user_id, event_id AS id,
           @@TS_EED_ENR@@                                         AS enrolled_on,
           certificate_id, NULL::VARCHAR AS badge_id,
           @@TS_EED_DONE@@                                        AS first_certificate_generated_on,
           coalesce(status, 'Not Active')                         AS user_consumption_status,
           NULL::BOOLEAN                                          AS live_cbp_plan_mandate,
           NULL::FLOAT                                            AS user_rating,
           TRY_CAST(completion_percentage AS DOUBLE)              AS content_progress_percentage
    FROM eed
    WHERE event_id NOT IN ('do_11419375418191052819', 'do_114193445681192960140', 'do_11419347470261452812',
                           'do_11419313252346265611', 'do_114193443002286080139')
),
asd AS (
    SELECT user_id AS asd_user_id, content_id AS asd_content_id, assessment_id AS asd_assessment_id,
           assessment_name AS asd_assessment_name, assessment_sub_type AS asd_assessment_sub_type,
           assessment_duration AS asd_assessment_duration, time_spent_by_the_user AS asd_time_spent_by_the_user,
           pass AS asd_pass, @@TS_ASM_DONE@@ AS asd_completion_date, score_achieved AS asd_score_achieved,
           overall_score AS asd_overall_score, cut_off_percentage AS asd_cut_off_percentage,
           total_question AS asd_total_question, number_of_incorrect_responses AS asd_number_of_incorrect_responses,
           number_of_retakes AS asd_number_of_retakes
    FROM asm
    WHERE assessment_sub_type IN ('Course Assessment', 'Comprehensive Assessment Program', 'Standalone Assessment')
)
SELECT
    me.user_id,
    me.id                                                         AS content_or_event_id,
    mc.name                                                       AS content_or_event_name,
    mc.duration                                                   AS duration,
    mc.type                                                       AS content_or_event_type,
    mc.sub_type                                                   AS content_or_event_sub_type,
    mc.content_or_event                                           AS content_or_event,
    mc.content_provider_name                                      AS content_provider_name,
    mc.content_status                                             AS content_status,
    coalesce(tr.tr_theme, 'Others')                               AS theme,
    tr.tr_theme_count                                             AS theme_count,
    CASE WHEN tr.tr_theme_count > 1 THEN 1 ELSE 0 END             AS multi_theme_flag,
    me.user_consumption_status,
    me.certificate_id, me.badge_id, me.first_certificate_generated_on, me.enrolled_on,
    me.live_cbp_plan_mandate, me.content_progress_percentage, me.user_rating,
    asd.asd_user_id                                               AS assessment_user_id,
    asd.asd_content_id                                            AS assessment_content_id,
    asd.asd_assessment_id                                         AS assessment_id,
    asd.asd_assessment_name                                       AS assessment_name,
    asd.asd_assessment_sub_type                                   AS assessment_sub_type,
    asd.asd_assessment_duration                                   AS assessment_duration,
    asd.asd_time_spent_by_the_user                                AS time_spent_by_the_user,
    asd.asd_pass                                                  AS pass,
    asd.asd_completion_date                                       AS completion_date,
    asd.asd_score_achieved                                        AS score_achieved,
    asd.asd_overall_score                                         AS overall_score,
    asd.asd_cut_off_percentage                                    AS cut_off_percentage,
    asd.asd_total_question                                        AS total_question,
    asd.asd_number_of_incorrect_responses                         AS number_of_incorrect_responses,
    asd.asd_number_of_retakes                                     AS number_of_retakes,
    CASE WHEN me.enrolled_on IS NOT NULL THEN 1 ELSE 0 END        AS enrolments,
    CASE WHEN length(me.certificate_id) != 0 THEN 1 ELSE 0 END    AS completion,
    CAST(CASE
        WHEN length(me.certificate_id) != 0
         AND mc.type IN ('Course', 'Event', 'External Content')
         AND (mc.sub_type != 'Case Study' OR mc.sub_type IS NULL)
        THEN mc.duration_hours
        ELSE 0
    END AS DECIMAL(12, 2))                                        AS learning_hours,
    CASE WHEN asd.asd_pass = 'Yes' AND asd.asd_user_id IS NOT NULL
          AND asd.asd_assessment_sub_type IN ('Course Assessment', 'Comprehensive Assessment Program', 'Standalone Assessment')
         THEN 1 ELSE 0 END                                        AS passed_users,
    CASE WHEN asd.asd_user_id IS NOT NULL
          AND asd.asd_assessment_sub_type IN ('Course Assessment', 'Comprehensive Assessment Program', 'Standalone Assessment')
         THEN 1 ELSE 0 END                                        AS total_attempts,
    CASE WHEN me.id = 'do_1145332932661821441231' AND me.badge_id IS NOT NULL THEN 'AI Daksh'
         WHEN mc.content_provider_name IN ('Coursera', 'Harvard Business Impact') AND me.badge_id IS NOT NULL THEN 'Utkarsh'
         ELSE NULL END                                            AS badge_type
FROM me
LEFT JOIN merged_content mc ON me.id = mc.id
LEFT JOIN theme_rollup   tr ON me.id = tr.tr_content_id
LEFT JOIN asd               ON me.user_id = asd.asd_user_id AND me.id = asd.asd_content_id;

-- ---------------------------------------------------------------------------
-- Stage 4: user_enrolments_custom = every active user, with their enrolment rows if any
--          (RIGHT JOIN keeps the small user table as the hash-join build side)
-- ---------------------------------------------------------------------------
COPY (
SELECT
    ud.user_id, ud.full_name, ud.designation, ud.email, ud.gender, ud.masked_email, ud.masked_phone_number,
    ud.phone_number, ud."groups", ud.user_registration_date, ud.tag, ud.mdo_id, ud.roles, ud.profile_status,
    ud.external_system_id, ud.external_system, ud.employee_id, ud.is_from_organised_service_of_govt,
    ud.civil_service_type, ud.civil_services, ud.cadre_batch, ud.cadre,
    f.content_or_event_id, f.content_or_event_name, f.duration, f.content_or_event_type,
    f.content_or_event_sub_type, f.content_or_event, f.content_provider_name, f.content_status,
    coalesce(f.theme, 'Others')                                       AS theme,
    f.theme_count,
    coalesce(f.multi_theme_flag, 0)                                   AS multi_theme_flag,
    coalesce(f.user_consumption_status, 'Not Active')                 AS user_consumption_status,
    f.certificate_id, f.badge_id, f.first_certificate_generated_on, f.enrolled_on,
    f.live_cbp_plan_mandate, f.content_progress_percentage, f.user_rating,
    ud.mdo_name, ud.ministry, ud.department, ud.ministry_mdo_id, ud.department_mdo_id, ud.Centre_State,
    f.assessment_user_id, f.assessment_content_id, f.assessment_id, f.assessment_name, f.assessment_sub_type,
    f.assessment_duration, f.time_spent_by_the_user, f.pass, f.completion_date, f.score_achieved,
    f.overall_score, f.cut_off_percentage, f.total_question, f.number_of_incorrect_responses, f.number_of_retakes,
    ud.registration_method,
    coalesce(f.enrolments, 0)                                         AS enrolments,
    coalesce(f.completion, 0)                                         AS completion,
    CASE WHEN f.user_id IS NULL THEN CAST(0 AS DECIMAL(12, 2)) ELSE f.learning_hours END AS learning_hours,
    coalesce(f.passed_users, 0)                                       AS passed_users,
    coalesce(f.total_attempts, 0)                                     AS total_attempts,
    f.badge_type
FROM facts f
RIGHT JOIN ud2 ud ON f.user_id = ud.user_id
) TO '@@W@@/user_enrolments_custom_new' (FORMAT PARQUET, COMPRESSION 'snappy', FILE_SIZE_BYTES '1GB', OVERWRITE_OR_IGNORE true);

DROP TABLE facts;

-- ---------------------------------------------------------------------------
-- Stage 5: the four extra tables, built from the user_enrolments_custom just written
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MACRO s(x) AS coalesce(CAST(x AS VARCHAR), '');                          -- NULL -> ''
CREATE OR REPLACE MACRO flag(cond) AS CAST(CASE WHEN cond THEN 1 ELSE 0 END AS INTEGER);   -- NULL -> 0

CREATE OR REPLACE VIEW uec AS SELECT * FROM read_parquet('@@W@@/user_enrolments_custom_new/*.parquet');

CREATE OR REPLACE TABLE per_user AS
SELECT user_id, CAST(sum(coalesce(CAST(completion AS BIGINT), 0)) AS BIGINT) AS total_completions
FROM uec
GROUP BY user_id;

-- mdo_enrolment_fact: one row per user_enrolments_custom row
COPY (
SELECT
    e.user_id                                                            AS user_id,
    s(e.designation)                                                     AS designation,
    s(e.gender)                                                          AS gender,
    s(e."groups")                                                        AS "groups",
    s(e.tag)                                                             AS tag,
    s(e.roles)                                                           AS roles,
    s(e.profile_status)                                                  AS profile_status,
    s(e.registration_method)                                             AS registration_method,
    e.user_registration_date                                             AS user_registration_date,
    s(e.employee_id)                                                     AS employee_id,
    s(e.external_system)                                                 AS external_system,
    s(e.external_system_id)                                              AS external_system_id,
    flag(strpos(s(e.email), '@gmail.com') > 0)                           AS is_gmail,
    s(e.civil_services)                                                  AS civil_services,
    s(e.cadre)                                                           AS cadre,
    s(e.mdo_id)                                                          AS mdo_id,
    s(e.mdo_name)                                                        AS mdo_name,
    s(e.ministry)                                                        AS ministry,
    s(e.department)                                                      AS department,
    s(e.ministry_mdo_id)                                                 AS ministry_mdo_id,
    s(e.department_mdo_id)                                               AS department_mdo_id,
    s(e.Centre_State)                                                    AS Centre_State,
    s(e.content_or_event_id)                                             AS content_or_event_id,
    s(e.content_or_event_name)                                           AS content_or_event_name,
    s(e.content_or_event_type)                                           AS content_or_event_type,
    s(e.content_or_event_sub_type)                                       AS content_or_event_sub_type,
    s(e.content_or_event)                                                AS content_or_event,
    s(e.content_provider_name)                                           AS content_provider_name,
    s(e.content_status)                                                  AS content_status,
    s(e.theme)                                                           AS theme,
    s(e.duration)                                                        AS duration,
    flag(coalesce(e.live_cbp_plan_mandate, false))                       AS live_cbp_plan_mandate,
    s(e.user_consumption_status)                                         AS user_consumption_status,
    s(e.certificate_id)                                                  AS certificate_id,
    e.enrolled_on                                                        AS enrolled_on,
    e.first_certificate_generated_on                                     AS first_certificate_generated_on,
    e.completion_date                                                    AS completion_date,
    coalesce(CAST(e.enrolments     AS INTEGER), 0)                       AS enrolments,
    coalesce(CAST(e.completion     AS INTEGER), 0)                       AS completion,
    coalesce(CAST(e.learning_hours AS DOUBLE),  0)                       AS learning_hours,
    coalesce(CAST(e.total_attempts AS INTEGER), 0)                       AS total_attempts,
    coalesce(CAST(e.passed_users   AS INTEGER), 0)                       AS passed_users,
    flag(s(e.content_or_event_id) != '')                                 AS is_active,
    flag(e.user_consumption_status = 'completed')                        AS is_completed,
    CAST(coalesce(u.total_completions, 0) AS BIGINT)                     AS user_total_completions
FROM uec e
LEFT JOIN per_user u ON u.user_id = e.user_id
) TO '@@W@@/mdo_enrolment_fact_new' (FORMAT PARQUET, COMPRESSION 'snappy', FILE_SIZE_BYTES '1GB', OVERWRITE_OR_IGNORE true);

-- mdo_user_summary: one row per user (any_value = first non-NULL value)
COPY (
SELECT
    user_id,
    s(full_name)                                                         AS full_name,
    s(designation)                                                       AS designation,
    s(gender)                                                            AS gender,
    s(email)                                                             AS email,
    s(phone_number)                                                      AS phone_number,
    flag(strpos(s(email), '@gmail.com') > 0)                             AS is_gmail,
    s("groups")                                                          AS "groups",
    user_registration_date,
    s(tag)                                                               AS tag,
    s(roles)                                                             AS roles,
    s(profile_status)                                                    AS profile_status,
    s(registration_method)                                               AS registration_method,
    s(external_system)                                                   AS external_system,
    s(external_system_id)                                                AS external_system_id,
    s(employee_id)                                                       AS employee_id,
    flag(lower(trim(CAST(is_from_organised_service_of_govt AS VARCHAR))) IN ('true', '1'))
                                                                         AS is_from_organised_service_of_govt,
    s(civil_service_type)                                                AS civil_service_type,
    s(civil_services)                                                    AS civil_services,
    s(cadre_batch)                                                       AS cadre_batch,
    s(cadre)                                                             AS cadre,
    s(mdo_id)                                                            AS mdo_id,
    s(mdo_name)                                                          AS mdo_name,
    s(ministry)                                                          AS ministry,
    s(department)                                                        AS department,
    s(ministry_mdo_id)                                                   AS ministry_mdo_id,
    s(department_mdo_id)                                                 AS department_mdo_id,
    s(Centre_State)                                                      AS Centre_State,
    enrolments, completion, learning_hours, total_attempts, passed_users, is_active, is_completed
FROM (
    SELECT
        user_id,
        any_value(full_name) AS full_name, any_value(designation) AS designation, any_value(gender) AS gender,
        any_value(email) AS email, any_value(phone_number) AS phone_number, any_value("groups") AS "groups",
        any_value(user_registration_date) AS user_registration_date, any_value(tag) AS tag,
        any_value(roles) AS roles, any_value(profile_status) AS profile_status,
        any_value(registration_method) AS registration_method,
        any_value(external_system) AS external_system, any_value(external_system_id) AS external_system_id,
        any_value(employee_id) AS employee_id,
        any_value(is_from_organised_service_of_govt) AS is_from_organised_service_of_govt,
        any_value(civil_service_type) AS civil_service_type, any_value(civil_services) AS civil_services,
        any_value(cadre_batch) AS cadre_batch, any_value(cadre) AS cadre,
        any_value(mdo_id) AS mdo_id, any_value(mdo_name) AS mdo_name, any_value(ministry) AS ministry,
        any_value(department) AS department, any_value(ministry_mdo_id) AS ministry_mdo_id,
        any_value(department_mdo_id) AS department_mdo_id, any_value(Centre_State) AS Centre_State,
        CAST(sum(coalesce(CAST(enrolments     AS BIGINT), 0)) AS BIGINT) AS enrolments,
        CAST(sum(coalesce(CAST(completion     AS BIGINT), 0)) AS BIGINT) AS completion,
        sum(coalesce(CAST(learning_hours AS DOUBLE), 0))                 AS learning_hours,
        CAST(sum(coalesce(CAST(total_attempts AS BIGINT), 0)) AS BIGINT) AS total_attempts,
        CAST(sum(coalesce(CAST(passed_users   AS BIGINT), 0)) AS BIGINT) AS passed_users,
        max(flag(s(content_or_event_id) != ''))                          AS is_active,
        max(flag(user_consumption_status = 'completed'))                 AS is_completed
    FROM uec
    GROUP BY user_id
)
) TO '@@W@@/mdo_user_summary_new' (FORMAT PARQUET, COMPRESSION 'snappy', FILE_SIZE_BYTES '1GB', OVERWRITE_OR_IGNORE true);

-- mdo_user_access: every user_detail row
COPY (
SELECT CAST(user_id AS VARCHAR) AS user_id, s(roles) AS roles, s(email) AS email, s(mdo_id) AS mdo_id
FROM udt
) TO '@@W@@/mdo_user_access_new' (FORMAT PARQUET, COMPRESSION 'snappy', FILE_SIZE_BYTES '1GB', OVERWRITE_OR_IGNORE true);

-- mdo_org_access: every L_org_hierarchy row
COPY (
SELECT CAST(mdo_id AS VARCHAR) AS mdo_id, s(ministry_mdo_id) AS ministry_mdo_id, s(department_mdo_id) AS department_mdo_id
FROM ohy
) TO '@@W@@/mdo_org_access_new' (FORMAT PARQUET, COMPRESSION 'snappy', FILE_SIZE_BYTES '1GB', OVERWRITE_OR_IGNORE true);
"""

# BigQuery's patterns (the dot is "any character") and the same patterns with the dot escaped
GOV_BIGQUERY = [".in$", ".org$", ".pfcindia.com$"]
GOV_ESCAPED = [r"\.in$", r"\.org$", r"\.pfcindia\.com$"]

WORK_DB = TMP + "/uec_work.duckdb"


def log(msg):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  {msg}", flush=True)


def has_parquet(folder):
    return os.path.isdir(folder) and any(f.endswith(".parquet") for f in os.listdir(folder))


def remove(path):
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)
    elif os.path.exists(path):
        os.remove(path)


def cleanup_new():
    for name in OUTPUTS:
        remove(f"{WAREHOUSE}/{name}_new")


def cleanup_work():
    for f in (WORK_DB, WORK_DB + ".wal"):
        remove(f)


def fail(msg):
    log(f"ERROR: {msg}")
    log("existing outputs left unchanged")
    cleanup_new()
    sys.exit(1)


def gov_expr(patterns, col):
    return " OR ".join(f"regexp_matches({col}, '{p}')" for p in patterns)


def date_expr(dtype, col):
    """SQL that turns a date column into a UTC timestamp (text, timestamps, or nanoseconds since 1970)."""
    if dtype.startswith(("BIGINT", "HUGEINT", "UBIGINT")):
        return f"make_timestamp({col} // 1000)"
    return f"TRY_CAST(TRY_CAST({col} AS TIMESTAMPTZ) AS TIMESTAMP)"


def main():
    start = time.time()
    log("user_enrolments_custom build started")

    lock = open("/tmp/build_user_enrolments_custom.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fail("another run is already in progress")

    os.makedirs(TMP, exist_ok=True)
    cleanup_work()
    cleanup_new()
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
        log(f"input {folder:<26} {n:>14,} rows")

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
        log(f"date  {folder}.{col:<34} {dtype}{note}")

    # 3. registration_method: how many users does the BigQuery pattern treat differently?
    differ_where = f"({gov_expr(GOV_BIGQUERY, 'email')}) != ({gov_expr(GOV_ESCAPED, 'email')})"
    active = "TRY_CAST(status AS INTEGER) = 1"
    ud_path = f"read_parquet('{WAREHOUSE}/user_detail/*.parquet')"
    differ = con.execute(f"SELECT count(*) FROM {ud_path} WHERE {active} AND {differ_where}").fetchone()[0]
    if differ:
        top = con.execute(
            f"SELECT lower(regexp_extract(email, '@(.*)$', 1)), count(*) FROM {ud_path} "
            f"WHERE {active} AND {differ_where} GROUP BY 1 ORDER BY 2 DESC LIMIT 5").fetchall()
        which = "Government Email (as BigQuery)" if REGISTRATION_LIKE_BIGQUERY else "Personal Email (as the Spark job)"
        log(f"registration_method: {differ:,} users differ between the BigQuery pattern and an escaped dot; "
            f"they get {which}. Top domains: " + ", ".join(f"{d or '?'} ({n:,})" for d, n in top))
    else:
        log("registration_method: BigQuery and escaped patterns give the same result for every user")

    # 4. build, one statement at a time
    live = ("CASE WHEN lower(CAST(live_cbp_plan_mandate AS VARCHAR)) IN ('true', '1', 'yes', 't') THEN true "
            "WHEN lower(CAST(live_cbp_plan_mandate AS VARCHAR)) IN ('false', '0', 'no', 'f') THEN false END")
    sql = SQL
    for key, expr in placeholders.items():
        sql = sql.replace(f"@@{key}@@", expr)
    sql = (sql.replace("@@GOV_EXPR@@", gov_expr(GOV_BIGQUERY if REGISTRATION_LIKE_BIGQUERY else GOV_ESCAPED, "ud.email"))
              .replace("@@LIVE_BOOL@@", live)
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
            label = "write " + os.path.basename(re.search(r"\)\s*TO\s+'([^']+)'", st).group(1)).replace("_new", "")
        elif head.startswith("DROP TABLE"):
            label = "free " + head.split()[2]
        t0 = time.time()
        try:
            con.execute(st)
        except Exception as e:  # noqa: BLE001
            fail(f"stage '{label or head[:50]}' failed: {e}")
        if label:
            extra = ""
            if label.startswith("table "):
                extra = f"{con.execute(f'SELECT count(*) FROM {head.split()[4]}').fetchone()[0]:>14,} rows"
            log(f"   {label:<36} {time.time() - t0:8.1f}s  {extra}")

    # 5. checks
    new = {name: f"read_parquet('{WAREHOUSE}/{name}_new/*.parquet')" for name in OUTPUTS}
    q = lambda sql_: con.execute(sql_).fetchone()  # noqa: E731
    uec_rows, uec_users = q(f"SELECT count(*), count(DISTINCT user_id) FROM {new['user_enrolments_custom']}")
    active_users = q("SELECT count(DISTINCT user_id) FROM ud2")[0]
    fact_rows = q(f"SELECT count(*) FROM {new['mdo_enrolment_fact']}")[0]
    sum_rows, sum_distinct = q(f"SELECT count(*), count(DISTINCT user_id) FROM {new['mdo_user_summary']}")
    access_rows = q(f"SELECT count(*) FROM {new['mdo_user_access']}")[0]
    org_rows = q(f"SELECT count(*) FROM {new['mdo_org_access']}")[0]
    if uec_rows == 0:
        fail("user_enrolments_custom has 0 rows")
    if uec_users != active_users:
        fail(f"user_enrolments_custom has {uec_users:,} users, but user_detail has {active_users:,} active users")
    if fact_rows != uec_rows:
        fail(f"mdo_enrolment_fact has {fact_rows:,} rows, user_enrolments_custom {uec_rows:,}")
    if sum_rows != sum_distinct or sum_rows != uec_users:
        fail(f"mdo_user_summary: {sum_rows:,} rows, {sum_distinct:,} distinct users, expected {uec_users:,} of each")
    if access_rows != q("SELECT count(*) FROM udt")[0]:
        fail("mdo_user_access does not have one row per user_detail row")
    if org_rows != q("SELECT count(*) FROM ohy")[0]:
        fail("mdo_org_access does not have one row per L_org_hierarchy row")

    log(f"result: {uec_rows:,} rows, {uec_users:,} users")
    totals = q(f"SELECT sum(enrolments), sum(completion), round(sum(learning_hours), 2), sum(passed_users), "
               f"sum(total_attempts), count(DISTINCT mdo_id), count(mdo_name), count(badge_type) "
               f"FROM {new['user_enrolments_custom']}")
    for name, v in zip(["sum enrolments", "sum completion", "sum learning_hours", "sum passed_users",
                        "sum total_attempts", "distinct mdo_id", "rows with mdo_name", "badge_type set"], totals):
        log(f"   {name:<22}{v!s:>18}")
    counts = {"user_enrolments_custom": uec_rows, "mdo_enrolment_fact": fact_rows, "mdo_user_summary": sum_rows,
              "mdo_user_access": access_rows, "mdo_org_access": org_rows}
    for name in OUTPUTS:
        cur = f"{WAREHOUSE}/{name}"
        if has_parquet(cur):
            prev = q(f"SELECT count(*) FROM read_parquet('{cur}/*.parquet')")[0]
            if prev and counts[name] < prev * (1 - MAX_DROP_PCT / 100):
                fail(f"{name} has {counts[name]:,} rows, the current output has {prev:,} (more than {MAX_DROP_PCT}% fewer)")
            log(f"   {name:<26}{counts[name]:>14,} rows   (current: {prev:,})")
        else:
            log(f"   {name:<26}{counts[name]:>14,} rows   (no current output)")
    con.close()

    # 6. replace the outputs
    for name in OUTPUTS:
        remove(f"{WAREHOUSE}/{name}")
        os.rename(f"{WAREHOUSE}/{name}_new", f"{WAREHOUSE}/{name}")
        files = [f for f in os.listdir(f"{WAREHOUSE}/{name}") if f.endswith(".parquet")]
        size = sum(os.path.getsize(f"{WAREHOUSE}/{name}/{f}") for f in files) / 1e9
        log(f"written {name}  ({len(files)} files, {size:.1f} GB)")
    log(f"finished in {(time.time() - start) / 60:.1f} min")


if __name__ == "__main__":
    main()
