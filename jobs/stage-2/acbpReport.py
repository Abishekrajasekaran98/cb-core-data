import findspark

findspark.init()
import time
import shutil
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType, BooleanType
from pyspark.sql.functions import col, desc, row_number, create_map, trim, array_join, split, \
    regexp_replace, when,lit, date_format, from_json, explode, broadcast, sum as spark_sum
from pyspark.sql.window import Window
from itertools import chain
from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))
from dfutil.content import contentDFUtil
from jobs.config import get_environment_config
from jobs.default_config import create_config

from constants.ParquetFileConstants import ParquetFileConstants

# ----------------------------------------------------------------------------
# Columns kept in the joined ACBP x content x enrolment intermediate: the union
# of what the enrolment report, the user summary and the APAR data read.
# Nothing else is carried through the join / window shuffles or written to disk.
# ----------------------------------------------------------------------------
CORE_COLS = [
    "userID", "courseID",
    # user details (come with acbp-computed)
    "fullName", "userPrimaryEmail", "userMobile", "userOrgName", "group", "designation",
    "ministry_name", "dept_name", "cadreName", "civilServiceType", "civilServiceName",
    "cadreBatch", "organised_service", "userOrgID", "employeeCode", "userStatus",
    # plan details
    "isapar", "acbpStatus", "allocatedOn", "completionDueDate", "planyear", "plantype", "isACBPMandatory",
    # content details
    "courseName", "courseStatus", "courseDuration", "courseCategory",
    # enrolment details
    "dbCompletionStatus", "courseCompletedTimestamp", "courseProgress",
]


class ACBPModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.ACBPModel"

    def name(self):
        return "ACBPModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    def process_data(self, spark, config):
        intermediatePath = None
        try:
            start_time = time.time()
            stage_start = time.time()
            today = self.get_date()

            # Evaluated ONCE and used as a literal, so every output of this run carries the
            # same "generated on" time (current_timestamp() is re-evaluated per Spark action).
            reportGeneratedOn = spark.sql(
                f"SELECT date_format(current_timestamp(), '{ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT}')"
            ).first()[0]
            currentDateTime = lit(reportGeneratedOn)

            primary_categories = ["Course", "Program", "Blended Program", "Curated Program", "Standalone Assessment"]

            print("📥 Reading source data...")
            # Only the APAR output needs anything from the user file (external system ids);
            # all other user details already come with acbp-computed.
            userAdditionalProperties = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE).select(
                "userID",
                col("additionalProperties.externalSystem").alias("externalSystem"),
                col("additionalProperties.externalSystemId").alias("externalSystemId"))

            allCourseProgramESDF = spark.read.parquet(
                ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE).filter(
                col("category").isin(primary_categories))

            allCourseProgramDetailsDF = (contentDFUtil.addCourseOrgDetails(
                allCourseProgramESDF,
                spark.read.parquet(ParquetFileConstants.ORG_SELECT_PARQUET_FILE))
                                         .fillna(0.0, subset=["courseDuration"])
                                         .fillna(0, subset=["courseResourceCount"]))

            course_schema = StructType([
                StructField("identifier", StringType(), True),
                StructField("mandatory", BooleanType(), True),
            ])
            course_id_regex = r"^\s*\[|\]\s*$|\s+"

            # Course IDs that appear in any Live plan, taken from the small (26 MB) acbp-select file
            # with the same parsing as below. acbp-computed is built from these same Live plans, so
            # an enrolment for any other course can never match the left join -> drop it up front.
            liveCourseIDsDF = (spark.read.parquet(ParquetFileConstants.ACBP_SELECT_FILE)
                               .filter(col("acbpStatus") == "Live")
                               .select(explode(col("acbpCourseIDList")).alias("c"))
                               .select(regexp_replace(from_json(col("c"), course_schema)["identifier"],
                                                      course_id_regex, "").alias("courseID"))
                               .na.drop()
                               .distinct())

            # 51 GB file: read only the columns used (the large per-content status maps are never
            # read), and keep only enrolments in courses that are part of a Live plan.
            enrolmentDF = (spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
                           .filter(col('enrolment_status') == 'enrolled')
                           .select("userID", "courseID", "dbCompletionStatus",
                                   "courseCompletedTimestamp", "courseProgress")
                           .join(broadcast(liveCourseIDsDF), ["courseID"], "left_semi"))

            # Every consumer of this data only uses Live plans, so filter BEFORE the explode and
            # the joins. Null userID/courseID rows are dropped here too (same as the old na.drop
            # after the joins: both columns come from this side of the left joins).
            acbpLiveDF = (spark.read.parquet(ParquetFileConstants.ACBP_COMPUTED_FILE)
                          .filter(col("acbpStatus") == "Live")
                          .withColumn("courseIDList", explode(col("acbpCourseIDList")))
                          .withColumn("courseIDList", from_json(col("courseIDList"), course_schema))
                          .withColumn("courseID", col("courseIDList.identifier"))
                          .withColumn("isACBPMandatory", col("courseIDList.mandatory"))
                          .withColumn("courseID", regexp_replace(col("courseID"), course_id_regex, ""))
                          .na.drop(subset=["userID", "courseID"])
                          .drop("acbpCourseIDList", "courseIDList"))

            # Latest-due plan per user/course: computed ONCE here and stored as row_num, so the
            # enrolment report and the user summary read the SAME chosen row.
            window_spec = Window.partitionBy("userID", "courseID").orderBy(desc("completionDueDate"))

            coreDF = (acbpLiveDF
                      .join(broadcast(allCourseProgramDetailsDF), ["courseID"], "left")
                      .join(enrolmentDF, ["courseID", "userID"], "left")
                      .withColumn("row_num", row_number().over(window_spec))
                      # Only rows an output reads: row_num = 1 (enrolment report + summary)
                      # or isapar (APAR uses all Live isapar rows). The rest were written for nothing.
                      .filter((col("row_num") == 1) | (col("isapar") == True))
                      .select(*CORE_COLS, "row_num"))

            # ------------------------------------------------------------------
            # Materialise the joined data ONCE as parquet. Before, this whole
            # pipeline (10 GB ACBP explode + 51 GB enrolment scan + join) was
            # recomputed for every output: enrolment plain parquet, enrolment
            # warehouse, user summary and APAR = 4 times.
            # ------------------------------------------------------------------
            intermediatePath = f"{config.localReportDir}/temp/acbp-report-intermediate/{today}"
            print(f"💾 Writing ACBP x enrolment intermediate -> {intermediatePath}")
            coreDF.write.mode("overwrite").option("compression", "snappy").parquet(intermediatePath)
            coreDF = spark.read.parquet(intermediatePath)
            print(f"Stage 1: Complete - joins + intermediate written (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            # ================= CB plan warehouse (from the small acbp-select file) =================
            assignment_type_mapping = {
                'rootorgid': 'mdo_id',
                'user': 'user',
                'customuser': 'user',
                'alluser': 'user',
                'designation': 'designation',
                'cadre': 'cadre',
                'group': 'groups',
                'batch': 'cadre_batch',
                'service': 'civil_services',
                'isprofileverified': 'is_verified_karmayogi',
                'isoncentraldeputation': 'is_on_central_deputation',
                'ministryorstateid': 'ministry_or_state_id'
            }

            mapping_expr = create_map([lit(x) for x in chain(*assignment_type_mapping.items())])

            # The old left join with content here was unused (no content column is selected,
            # and dropDuplicates() below removes any fan-out), so it is dropped.
            acbpSelectEnrolmentDF = spark.read.parquet(ParquetFileConstants.ACBP_SELECT_FILE) \
                .withColumn("courseIDList", explode(col("acbpCourseIDList"))) \
                .withColumn("courseIDList", from_json(col("courseIDList"), course_schema)) \
                .withColumn("courseID", col("courseIDList.identifier")) \
                .withColumn("isACBPMandatory", col("courseIDList.mandatory")) \
                .drop("acbpCourseIDList", "courseIDList") \
                .withColumn("assignmentTypeInfo",
                            array_join(
                                F.transform(
                                    split(col("assignmentTypeInfo"), "\\|"),
                                    lambda seg: array_join(
                                        F.transform(
                                            from_json(seg, ArrayType(StringType())),
                                            lambda v: F.concat(lit('"'), v, lit('"'))),", ")), "|")) \
                .withColumn("assignmentTypeInfo", when(col("assignmentType") == "alluser", lit("AllUser")).otherwise(col("assignmentTypeInfo"))) \
                .withColumn("assignmentType", array_join(F.transform(split(col("assignmentType"), "\\|"), lambda x: mapping_expr[trim(x)]), "|"))

            cbPlanWarehouseDF = acbpSelectEnrolmentDF \
                .select(
                "orgID", "acbpCreatedBy", "acbpID", "cbPlanName", "isapar",
                "assignmentType", "assignmentTypeInfo", "courseID",
                "allocatedOn", "completionDueDate", "acbpStatus", "planyear", "plantype", "isACBPMandatory"
            ) \
                .withColumn("data_last_generated_on", currentDateTime) \
                .select(
                col("orgID").alias("org_id"),
                col("acbpCreatedBy").alias("created_by"),
                col("acbpID").alias("cb_plan_id"),
                col("cbPlanName").alias("plan_name"),
                col("assignmentType").alias("allotment_type"),
                col("assignmentTypeInfo").alias("allotment_to"),
                col("courseID").alias("content_id"),
                col("isACBPMandatory").alias("isMandatory"),
                date_format(col("allocatedOn"), ParquetFileConstants.DATE_TIME_FORMAT).alias("allocated_on"),
                date_format(col("completionDueDate"), ParquetFileConstants.DATE_TIME_FORMAT).alias("due_by"),
                col("acbpStatus").alias("status"),
                col("isapar"),
                col("planyear").alias("plan_year"),
                col("plantype").alias("plan_type"),
                col("data_last_generated_on")
            ) \
                .dropDuplicates() \
                .orderBy("org_id", "created_by", "plan_name")

            # ================= Enrolment report =================
            acbpEnrolmentDF = coreDF.filter(col("row_num") == 1).drop("row_num")

            ministry_is_empty = (col("ministry_name").isNull()) | (col("ministry_name") == "")
            dept_is_empty = (col("dept_name").isNull()) | (col("dept_name") == "")

            enrolmentReportDF = acbpEnrolmentDF \
                .filter(col("userStatus").cast("int") == 1) \
                .select(
                "fullName", "userPrimaryEmail", "userMobile", "userOrgName", "group",
                "designation", "ministry_name", "dept_name", "cadreName", "civilServiceType", "civilServiceName",
                "cadreBatch", "organised_service", "courseName", "isapar",
                "userOrgID", "dbCompletionStatus", "courseCompletedTimestamp",
                "allocatedOn", "completionDueDate", "employeeCode",
                "planyear", "plantype", "isACBPMandatory"
            ) \
                .withColumn(
                "currentProgress",
                F.when(col("dbCompletionStatus") == 2, "Completed")
                .when(col("dbCompletionStatus") == 1, "In Progress")
                .when(col("dbCompletionStatus") == 0, "Not Started")
                .otherwise("Not Enrolled")
            ) \
                .withColumn("courseCompletedTimestamp",
                            date_format(col("courseCompletedTimestamp"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("allocatedOn", date_format(col("allocatedOn"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("completionDueDate",
                            date_format(col("completionDueDate"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("MDO_Name", col("userOrgName")) \
                .withColumn(
                "Ministry",
                when(ministry_is_empty, col("userOrgName")).otherwise(col("ministry_name"))
            ) \
                .withColumn(
                "Department",
                when(
                    (col("userOrgName").isNotNull()) &
                    (col("ministry_name") != col("userOrgName")) &
                    dept_is_empty,
                    col("userOrgName")
                ).otherwise(col("dept_name"))
            ) \
                .withColumn(
                "Organization",
                when(
                    (col("ministry_name") != col("userOrgName")) &
                    (col("dept_name") != col("userOrgName")),
                    col("userOrgName")
                ).otherwise(lit(""))
            ) \
                .select(
                col("fullName").alias("Name"),
                col("userPrimaryEmail").alias("Email"),
                col("userMobile").alias("Phone"),
                col("employeeCode").alias("EmployeeId"),
                col("MDO_Name"),
                col("group").alias("Group"),
                col("designation").alias("Designation"),
                col("Ministry"),
                col("Department"),
                col("Organization"),
                col("cadreName").alias("Cadre"),
                col("civilServiceType").alias("Civil Service Type"),
                col("civilServiceName").alias("Civil Services"),
                col("cadreBatch").alias("Cadre Batch"),
                col("organised_service").alias("Is From Organised Service of Govt"),
                col("courseName").alias("Name of CBP Allocated Course"),
                col("isapar"),
                col("allocatedOn").alias("Allocated On"),
                col("currentProgress").alias("Current Progress"),
                col("completionDueDate").alias("Due Date of Completion"),
                col("courseCompletedTimestamp").alias("Actual Date of Completion"),
                col("planyear").alias("Plan year"),
                col("plantype").alias("Plan type"),
                col("isACBPMandatory").alias("is mandatory"),
                col("userOrgID").alias("mdoid"),
                currentDateTime.alias("Report_Last_Generated_On")
            ) \
                .fillna("")

            # ================================================================
            # Plain parquet only - the per-org CSV split happens in the
            # separate polars_csv_export.py run after this job has stopped.
            # This path is STABLE - keep it in sync with polars_csv_export.py.
            # ================================================================
            print("📝 Writing plain parquet for enrollment report...")
            enrolmentReportPlainPath = f"{config.localReportDir}/temp/cbp-enrolment-report-plain/{today}"
            enrolmentReportDF.write.mode("overwrite").option("compression", "snappy").parquet(enrolmentReportPlainPath)

            # Warehouse copy is read back from the parquet just written instead of
            # recomputing enrolmentReportDF a second time.
            # Byte-for-byte file copy: no Spark read, decompress, recompress, write.
            warehouseEnrolmentPath = f"{config.warehouseReportDir}/cbp_enrollments"
            shutil.rmtree(warehouseEnrolmentPath, ignore_errors=True)
            shutil.copytree(enrolmentReportPlainPath, warehouseEnrolmentPath)
            print(f"Stage 2: Complete - enrollment plain parquet + warehouse write (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            # ================= APAR enrolment data =================
            print("📝 Start Apar enrollment report data...")

            kcmDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwKcmDictionaryTable}")
            kcmMappingDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwKcmContentTable}")

            kcmMappingDF = kcmMappingDF.join(kcmDF, kcmDF.competency_area_id == kcmMappingDF.competency_area_id, "left").select(
                col("course_id"),
                kcmMappingDF["competency_area_id"],
                col("competency_area")
            ).distinct()

            km = kcmMappingDF.alias("km")
            kc = (
                kcmDF
                .alias("kc")
                .withColumnRenamed("competency_area", "kc_competency_area")
            )

            resultDF = (
                km.join(kc, kc.competency_area_id == km.competency_area_id, "left")
                .select(
                    km.course_id,
                    km.competency_area_id,
                    kc.kc_competency_area.alias("competency_area")
                )
                .distinct()
                .groupBy("course_id")
                .agg(
                    F.concat_ws(", ", F.collect_set("competency_area")).alias("competency_areas")
                )
            )


            print("📝 Preparing Apar enrollment report data...")

            # Same rows as before: all Live plan rows (not only row_num = 1) with isapar = true.
            aparEnrolmentData = coreDF.where(col("isapar") == True) \
                .join(userAdditionalProperties, "userID", "left") \
                .join(resultDF, col("courseID") == resultDF.course_id, "left") \
                .withColumn(
                "content_duration",
                F.when(F.col("courseDuration").isNull(), None)
                .when(F.col("courseDuration") == 0, "00:00:00")
                .otherwise(
                    F.format_string(
                        "%02d:%02d:%02d",
                        (F.col("courseDuration") / 3600).cast("int"),
                        ((F.col("courseDuration") % 3600) / 60).cast("int"),
                        (F.col("courseDuration") % 60).cast("int")
                    )
                )
            ) \
                .filter((col("userStatus").cast("int") == 1) & (col("isApar") == True)) \
                .select(
                col("userID").alias("user_id"),
                col("fullName").alias("name"),
                col("employeeCode").alias("EmployeeId"),
                col("userOrgID").alias("mdo_id"),
                col("userOrgName").alias("mdo_name"),
                col("courseID").alias("content_id"),
                col("courseName").alias("content_name"),
                col("courseStatus").alias("content_status"),
                col("content_duration"),
                col("courseCategory").alias("content_type"),
                col("userMobile").alias("phone"),
                col("userPrimaryEmail").alias("email"),
                col('isApar').alias("is_apar"),
                when(((col("courseProgress").isNotNull()) & (col("courseProgress") > 0)), col("courseProgress")).otherwise(0).alias("content_progress_percentage"),
                col("externalSystem").alias("external_system"),
                col("externalSystemId").alias("external_system_id"),
                col("competency_areas").alias("competency_type"),
                lit(None).cast("string").alias("parichay_id"),
                col("allocatedOn").cast("timestamp").alias("assigned_on"),
                col("planyear").alias("plan_year"),
                col("isACBPMandatory").alias("isMandatory")
            ).dropDuplicates(["user_id", "content_id"])

            print("✅ Apar enrollment report data prepared successfully!")
            print(f"Stage 3: Complete - Apar enrollment data prepared (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            # ================= User summary report =================
            cleanDF = acbpEnrolmentDF \
                .filter(col("userStatus").cast("int") == 1) \
                .withColumn(
                "Ministry",
                when(ministry_is_empty, col("userOrgName")).otherwise(col("ministry_name"))
            ) \
                .withColumn(
                "Department",
                when(
                    (col("userOrgName").isNotNull()) &
                    (col("ministry_name") != col("userOrgName")) &
                    dept_is_empty,
                    col("userOrgName")
                ).otherwise(col("dept_name"))
            ) \
                .withColumn(
                "Organization",
                when(
                    (col("ministry_name") != col("userOrgName")) &
                    (col("dept_name") != col("userOrgName")),
                    col("userOrgName")
                ).otherwise(lit(""))
            )

            userSummaryReportDF = cleanDF \
                .groupBy(
                "userID", "fullName", "userPrimaryEmail", "userMobile",
                "designation", "cadreName", "civilServiceType", "civilServiceName",
                "cadreBatch", "organised_service", "group", "userOrgID",
                "Ministry", "Department", "Organization", "employeeCode"
            ) \
                .agg(
                # row_num == 1 leaves exactly one row per (userID, courseID) and userID is a group
                # key, so a plain count equals countDistinct without the costly distinct aggregation.
                F.count("courseID").alias("allocatedCount"),
                spark_sum(when(col("dbCompletionStatus") == 2, 1).otherwise(0)).alias("completedCount"),
                spark_sum(
                    when(
                        (col("dbCompletionStatus") == 2) &
                        (col("courseCompletedTimestamp").cast(LongType()) <
                         (col("completionDueDate").cast(LongType()) + 86400)),
                        1
                    ).otherwise(0)
                ).alias("completedBeforeDueDateCount")
            ) \
                .select(
                col("fullName").alias("Name"),
                col("userPrimaryEmail").alias("Email"),
                col("userMobile").alias("Phone"),
                col("employeeCode").alias("EmployeeId"),
                col("Ministry"),
                col("Department"),
                col("Organization"),
                col("group").alias("Group"),
                col("designation").alias("Designation"),
                col("cadreName").alias("Cadre"),
                col("civilServiceType").alias("Civil Service Type"),
                col("civilServiceName").alias("Civil Services"),
                col("cadreBatch").alias("Cadre Batch"),
                col("organised_service").alias("Is From Organised Service of Govt"),
                col("allocatedCount").alias("Number of CBP Courses Allocated"),
                col("completedCount").alias("Number of CBP Courses Completed"),
                col("completedBeforeDueDateCount").alias("Number of CBP Courses Completed within due date"),
                col("userOrgID").alias("mdoid"),
                currentDateTime.alias("Report_Last_Generated_On")
            )

            print("📝 Writing plain parquet for user summary report...")
            summaryReportPlainPath = f"{config.localReportDir}/temp/cbp-summary-report-plain/{today}"
            userSummaryReportDF.write.mode("overwrite").option("compression", "snappy").parquet(summaryReportPlainPath)
            print(f"Stage 4: Complete - user summary plain parquet written (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            print("📦 Writing warehouse data...")
            cbPlanWarehouseDF.write.mode("overwrite").option("compression", "snappy").parquet(
                f"{config.warehouseReportDir}/{config.dwCBPlanTable}")
            print(f"Stage 5: Complete - cbPlanWarehouseDF written (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            print("📝 Writing Apar enrollment parquet report for warehouse...")
            aparEnrolmentData.write.mode("overwrite").option("compression", "snappy").parquet(
                f"{config.warehouseReportDir}/{config.dwAparCBPEnrollmentTable}")
            print(f"Stage 6: Complete - aparEnrolmentData written (⏱ {time.time() - stage_start:.2f}s)")

            shutil.rmtree(intermediatePath, ignore_errors=True)

            total_time = time.time() - start_time
            print(f"\n✅ ACBPModel processing completed in {total_time:.2f} seconds ({total_time / 60:.1f} minutes)")
            print("   Next step: run polars_csv_export.py to produce the per-org CSVs from the plain parquet(s) above.")

        except Exception as e:
            print(f"❌ Error occurred during ACBPModel processing: {str(e)}")
            raise e


def main():
    spark = SparkSession.builder \
        .appName("ACBP Report") \
        .config("spark.master", "local[*]") \
        .config("spark.sql.shuffle.partitions", "256") \
        .config("spark.driver.memory", "120g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024)) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    config_dict = get_environment_config()
    config = create_config(config_dict)

    start_time = datetime.now()
    print(f"[START] ACBPModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = ACBPModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    print(f"[END] ACBPModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {end_time - start_time}")
    spark.stop()


if __name__ == "__main__":
    main()
