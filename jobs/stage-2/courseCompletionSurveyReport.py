import findspark
findspark.init()

import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, current_timestamp, date_format,lit, first, encode, explode)
from datetime import datetime
from pyspark.sql import functions as F
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.dfexport import dfexportutil
from jobs.default_config import create_config
from jobs.config import get_environment_config



class CourseCompletionSurveyReport:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.CourseBasedAssessmentModel"

    def name(self):
        return "CourseBasedAssessmentModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    def process_data(self, spark,config):
        try:
            start_time = time.time()
            today = self.get_date()
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)
            print("Stage 1: Loading course completion survey data...")
            courseCompletionSurveyDF = spark.read.parquet(ParquetFileConstants.COURSE_COMPLETION_SURVEY_PARQUET_FILE).filter(col("formId").isin(config.completionSurveyFormIds))
            print("Stage 1: Complete")
            # Stage 2: Exploding of Array
            print("Stage 2: Exploding the response Array")
            explodedsurveyDF = courseCompletionSurveyDF.withColumn(
                    "response",
                    explode(col("responses"))
                ).select(
                    "formId",
                    "contextId",
                    "contextName",
                    "version",
                    "status",
                    "submittedBy",
                    "submittedDate",
                    col("response.question").alias("question"),
                    col("response.answer").alias("answer")
                    )
            print("Stage 2: Complete")

            #Changing of rows to Columns and getting first values
            print("Stage 3: Pivot")
            reportDF = (
                    explodedsurveyDF
                    .groupBy("formId", "contextId", "contextName", "version","status", "submittedBy","submittedDate")
                    .pivot("question")
                    .agg(first("answer"))
                )
            reportDF = reportDF.withColumn("contextName", encode("contextName", "UTF-8"))

            print("Stage 3: Complete")
            #writing warehouse file 
            print("Stage 4: Writing Warehouse file")
            warehouseDF = reportDF.filter(col("formId").isin(config.contentEndSurveyFormid)).withColumn("data_last_generated_on", currentDateTime)\
                .select(
                   col("submittedBy").alias("user_id"),
                   col("contextId").alias("course_id"),
                   col("submittedDate").alias("survey_submitted_on"),
                   col("`1. The course met my expectations in its - Design`")
                      .cast("int")
                      .alias("design_rating"),
                   col("`2. The course met my expectations in its - Content`")
                      .cast("int")
                      .alias("content_rating"),
                   col("`3. The course met my expectations in its - Delivery`")
                     .cast("int")
                     .alias("delivery_rating"),
                   col("`4. The course met my expectations in its - Role Relevance`")
                      .cast("int")
                     .alias("role_relevance_rating"),
                   col("`What would you like to improve about this course? (e.g., course duration, pace, clarity, engagement, assessments, or any other aspect)`")
                     .alias("improvement_suggestions"),
                   lit(True).cast("boolean").alias("is_mandatory"),
                   col("version").alias("survey_version"),
                   col("data_last_generated_on"))

            (warehouseDF
               .write
               .mode("overwrite")
               .option("compression", "snappy")
               .parquet(f"{config.warehouseReportDir}/{config.dwCourseCompletionSurveryTable}"))

            print("Stage4 : Complete")
            coursecompletionpath = f"{config.localReportDir}/temp/course-completion-survey-report/{today}"
            reportDF.write.mode("overwrite").option("compression", "snappy").parquet(coursecompletionpath)
            # dfexportutil.write_csv_per_mdo_id_duckdb(
            #     reportDF,
            #     f"{config.localReportDir}/{config.courseCompletionSurveyPath}/{today}",
            #     'formId',
            #     f"{config.localReportDir}/temp/course-completion-survey-report/{today}",
            #     csv_filename=config.completionSurveyReport
            #    )
            total_time = time.time() - start_time
            print(
                f"\n✅ Optimized Course Completion Survey Report generation completed in {total_time:.2f} seconds ({total_time / 60:.1f} minutes)")

        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise

def main():
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("Course Completion Survey Report Model") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "18g") \
        .config("spark.driver.memory", "18g") \
        .config("spark.driver.maxResultSize", "3g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    # Create model instance
    start_time = datetime.now()
    print(f"[START] Course completion survey processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = CourseCompletionSurveyReport()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Course completion survey completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()
if __name__ == "__main__":
    main()
 