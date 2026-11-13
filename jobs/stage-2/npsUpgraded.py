import findspark

findspark.init()

import json
import time
from pathlib import Path
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType, \
    DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, row_number, struct, lit, to_json, count, current_timestamp,
                                   current_date, date_format, broadcast, desc, unix_timestamp, when, sum, collect_list,
                                   max, lit, concat_ws, from_unixtime, format_string, expr, coalesce, dense_rank,
                                   add_months, last_day)
from datetime import datetime, timedelta
import sys
import os
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.utils import utils
from jobs.default_config import create_config
from jobs.config import get_environment_config

# C1 (Druid) is the "already submitted / rejected" list. If Druid fails or returns nothing, carrying on would
# create a new feed for users who already answered, so the job stops instead.
# Set FAIL_IF_C1_EMPTY to False only where nobody can have submitted yet (for example a fresh test environment).
FAIL_IF_C1_EMPTY = True
C1_ROW_LIMIT = 1000000

# Time-based UUID (Cassandra timeuuid) -> milliseconds since 1970, using built-in Spark functions.
# The UUID text is  time_low(1-8) - time_mid(10-13) - version+time_hi(15-18) ; the 60-bit timestamp is
# time_hi(3 hex digits, without the version digit) + time_mid + time_low, counted in 100ns ticks since 1582-10-15.
# 122192928000000000 (= 0x01b21dd213814000) is the number of ticks between 1582-10-15 and 1970-01-01.
# Same result as uuid.UUID(u).time, without a Python function running on every row.
TIMEUUID_TO_MILLIS_EXPR = (
    "(cast(conv(concat(substring(createdon, 16, 3), substring(createdon, 10, 4), substring(createdon, 1, 8)), "
    "16, 10) as bigint) - 122192928000000000) div 10000"
)


class NPSUpgradedModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.leaderboard.LearnerLeaderBoardModel"

    def name(self):
        return "NPSUpgradedModel"

    def _window(self):
        """The 15-day window [start, end) as datetimes. Fixed once per run, so C2 and C3 always use the same days."""
        if getattr(self, "_window_cache", None) is None:
            window_end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            self._window_cache = (window_end - timedelta(days=15), window_end)
        return self._window_cache

    def process_data(self, spark, config):
        try:
            self._window()  # fix the window before anything reads it

            users_submitted_rejected_df = self.npsUpgradedTriggerC1DataFrame(spark, config)  # has userid who submitted/rejected in last 15 days
            # C2 and C3 are used by several steps below. Keep them in memory/disk so they are computed once.
            users_enrolled_completed_df = self.npsUpgradedTriggerC2DataFrame(spark, config).persist(StorageLevel.MEMORY_AND_DISK)  # has userid who enrolled/completed
            users_rated_course_df = self.npsUpgradedTriggerC3DataFrame(spark, config).persist(StorageLevel.MEMORY_AND_DISK)  # has userid who rated

            print(f"DataFrame Count (submitted/rejected in last 15 days): {users_submitted_rejected_df.count()}")
            print(f"DataFrame Count (enrolled or completed in last 15 days): {users_enrolled_completed_df.count()}")
            print(f"DataFrame Count (rated at least one course in last 15 days): {users_rated_course_df.count()}")

            # Eligible = (C2 ∪ C3) distinct userids
            df = users_enrolled_completed_df.select("userid").unionByName(
                users_rated_course_df.select("userid")
            ).dropDuplicates(["userid"]).na.drop(subset=["userid"])

            # Remove users who already submitted/rejected (C1)
            # Use set subtraction on the single key column to mirror Scala `except`
            filtered_df = df.select("userid").subtract(
                users_submitted_rejected_df.select("userid")
            ).na.drop(subset=["userid"])

            total_count = df.count()
            print(f"DataFrame Count (eligible users): {total_count}")

            filtered_count = filtered_df.count()
            print(f"DataFrame Count (eligible & not filled form): {filtered_count}")

            # Check existing feed (read once and keep, instead of re-reading Cassandra for every step)
            cassandra_df = self.userUpgradedFeedFromCassandraDataFrame(spark, config).select("userid").dropDuplicates(["userid"]).persist(StorageLevel.MEMORY_AND_DISK)
            existing_feed_count = cassandra_df.count()
            print(f"DataFrame Count (users already having feed): {existing_feed_count}")

            store_to_cassandra_df = filtered_df.select("userid").subtract(cassandra_df)
            filtered_store_to_cassandra_df = (
                store_to_cassandra_df
                .filter(
                    (col("userid").isNotNull()) &
                    (col("userid") != "") &
                    (col("userid") != "''")
                )
                .dropDuplicates(["userid"])
            )

            # Same text as before, e.g. {"dataValue":"yes","actionData":{"formId":123}}.
            # int() stops the job early if the survey id is not a number (the old f-string would write broken JSON).
            action_json = json.dumps(
                {"dataValue": "yes", "actionData": {"formId": int(config.platformRatingSurveyId)}},
                separators=(",", ":"))

            additional_df = (
                filtered_store_to_cassandra_df
                .withColumn("category", lit("NPS2"))
                .withColumn("id", expr("uuid()").cast(StringType()))
                .withColumn("createdby", lit("platform_rating"))
                .withColumn("createdon", current_date())
                .withColumn("action", lit(action_json))
                .withColumn("expireon", lit(None).cast(DateType()))
                .withColumn("priority", lit(1))
                .withColumn("status", lit("unread"))
                .withColumn("updatedby", lit(None).cast(StringType()))
                .withColumn("updatedon", lit(None).cast(DateType()))
                .withColumn("version", lit("v1"))
            ).persist(StorageLevel.MEMORY_AND_DISK)

            # uuid() is random. Computing it once and keeping the result means both tables below get the SAME ids.
            final_feed_count = additional_df.count()
            print(f"DataFrame Count (final users to create feed): {final_feed_count}")

            if final_feed_count == 0:
                print("[SUCCESS] NpsUpgradeModel completed: no new users need a feed, nothing written")
                return

            utils.writeToCassandra(additional_df, config.cassandraUserFeedKeyspace, config.cassandraUserFeedTable)
            utils.writeToCassandra(additional_df, "sunbird_notifications", "notification_feed_history")

            print("[SUCCESS] NpsUpgradeModel completed")

        except Exception as e:
            print(f"Error occurred during NPSUpgradedModel processing: {str(e)}")
            raise

    def nps_userids_schema(self):
        return StructType([StructField("userid", StringType(), True)])

    def userUpgradedFeedFromCassandraDataFrame(self, spark, config):
        """Users already having NPS2 feed in user_feed table (Cassandra)."""
        df = utils.read_cassandra_table(spark, config.cassandraUserFeedKeyspace, config.cassandraUserFeedTable)\
            .select(col("userid").alias("userid"))\
            .where(col("category") == "NPS2")
        if df is None:
            return spark.createDataFrame([], self.nps_userids_schema())
        return df.na.drop(subset=["userid"])

    def npsUpgradedTriggerC1DataFrame(self, spark, config):
        """Users who saw the upgraded NPS popup in last 15 days (from Druid)."""
        query = """SELECT userID as userid FROM "nps-upgraded-users-data" WHERE __time >= CURRENT_TIMESTAMP - 
        INTERVAL '15' DAY"""
        df = utils.druidDFOption(query, config.sparkDruidRouterHost, limit=C1_ROW_LIMIT, spark=spark)
        if df is None:
            # druidDFOption returns None both for "Druid failed" and for "no rows". Carrying on would treat
            # everybody as "has not answered yet", so stop (the reason is printed just above by druidDFOption).
            if FAIL_IF_C1_EMPTY:
                raise RuntimeError(
                    "Druid returned no data for the 'already submitted/rejected' list (C1). Stopping so that users "
                    "who already answered do not get a new NPS feed. Set FAIL_IF_C1_EMPTY = False only if an "
                    "empty list is expected here.")
            print("WARNING: C1 (already submitted/rejected) is empty; continuing because FAIL_IF_C1_EMPTY is False")
            return spark.createDataFrame([], self.nps_userids_schema())
        c1_rows = df.count()
        if c1_rows >= C1_ROW_LIMIT:
            print(f"WARNING: C1 returned {c1_rows} rows, which is the query limit ({C1_ROW_LIMIT}). The list may be "
                  f"cut off, so some users who already answered could get a new feed. Raise the limit.")
        return df.na.drop(subset=["userid"])  # ensure clean ids

    def npsUpgradedTriggerC2DataFrame(self, spark, config):
        """Users enrolled/completed at least 1 course in last 15 days (from Cassandra)."""
        window_start, window_end = self._window()
        enrolment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)\
            .filter(((col("firstCompletedOn") >= lit(window_start)) & (col("firstCompletedOn") < lit(window_end)))
                    | ((col("courseEnrolledTimestamp") >= lit(window_start))
                       & (col("courseEnrolledTimestamp") < lit(window_end)))
                    ).select("userid").distinct()
        return enrolment_df

    def npsUpgradedTriggerC3DataFrame(self, spark, config):
        """Users who rated at least one course in last 15 days (rating.createdon is timeuuid)."""
        window_start, window_end = self._window()
        today_start_ms = int(window_end.timestamp() * 1000)
        fifteen_days_ago_start_ms = int(window_start.timestamp() * 1000)
        ratings_df = spark.read.parquet(ParquetFileConstants.RATING_PARQUET_FILE)\
            .filter(col("createdon").isNotNull())\
            .withColumn("rated_on", expr(TIMEUUID_TO_MILLIS_EXPR))\
            .where((col("rated_on") >= lit(fifteen_days_ago_start_ms)) & (col("rated_on") < lit(today_start_ms)))\
            .select("userid")\
            .distinct()

        return ratings_df

def create_spark_session_with_packages(config):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'
    spark = SparkSession.builder \
        .appName("NPS Upgraded Model - Cached") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "18g") \
        .config("spark.driver.memory", "18g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.cassandra.connection.host", config.sparkCassandraConnectionHost) \
        .config("spark.cassandra.connection.port", '9042') \
        .config("spark.cassandra.output.batch.size.rows", '10000') \
        .config("spark.cassandra.connection.keepAliveMS", "60000") \
        .config("spark.cassandra.connection.timeoutMS", '30000') \
        .config("spark.cassandra.read.timeoutMS", '30000') \
        .getOrCreate()
    return spark


def main():
    try:
        # Create model instance
        start_time = datetime.now()
        print(f"[START] NPS processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        config_dict = get_environment_config()
        config = create_config(config_dict)
        spark = create_spark_session_with_packages(config)
        model = NPSUpgradedModel()
        model.process_data(spark, config)
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"[END] NPS completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Total duration: {duration}")
    except Exception as e:
        print(f"[ERROR] NPS job failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()