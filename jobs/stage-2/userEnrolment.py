import findspark

findspark.init()
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType, IntegerType, StructType, StructField, BooleanType
from pyspark.sql.functions import col, when, size, lit, expr, date_format, current_timestamp, \
    to_date, round, explode, to_utc_timestamp, from_utc_timestamp, to_timestamp, upper, concat, \
    row_number, trim, exists, explode_outer, coalesce, format_string, split, from_unixtime, regexp_replace, from_json
from pyspark.sql.window import Window

from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))
from dfutil.enrolment import enrolmentDFUtil
from dfutil.dfexport import dfexportutil
from dfutil.assessment import assessmentDFUtil

from constants.ParquetFileConstants import ParquetFileConstants
from jobs.default_config import create_config
from jobs.config import get_environment_config


class UserEnrolmentModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.UserEnrolmentModel"

    def name(self):
        return "UserEnrolmentModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def current_date_time():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def duration_format(df, in_col, out_col=None):
        out_col_name = out_col if out_col is not None else in_col

        return df.withColumn(out_col_name,
                             when(col(in_col).isNull(), lit(""))
                             .otherwise(
                                 format_string("%02d:%02d:%02d",
                                               expr(f"{in_col} / 3600").cast("int"),
                                               expr(f"{in_col} % 3600 / 60").cast("int"),
                                               expr(f"{in_col} % 60").cast("int")
                                               )
                             )
                             )

    def process_data(self, spark, config):
        try:
            today = self.get_date()
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)

            print("📥 Loading base DataFrames...")
            primary_categories = ["Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"]

            # Load and cache base DataFrames that are used multiple times
            unenrolmentAuditDF = spark.read.parquet(ParquetFileConstants.UNENROLMENT_AUDIT_PARQUET_FILE) \
                .filter(col('action') == 'UNENROLL') \
                .select(
                col("userid").alias("userID"),
                col("courseid").alias("courseID"),
                col("batchid").alias("batchID"),
                col("actiondate").alias("unenrolledOn"),
                col("action")
            ).cache()

            # A user/course/batch can have more than one unenrol record (e.g. unenrolled,
            # re-enrolled, unenrolled again). Keep only the most recent one so the join
            # below can't fan out and duplicate rows in the enrolment report.
            unenrolmentAuditLatestDF = (
                unenrolmentAuditDF
                .withColumn(
                    "rn",
                    row_number().over(
                        Window.partitionBy("userID", "courseID", "batchID").orderBy(col("unenrolledOn").desc())
                    )
                )
                .filter(col("rn") == 1)
                .drop("rn")
            )

            enrolmentDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
            userOrgDF = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
            contentOrgDF = spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE).filter(
                col("category").isin(primary_categories))
            # ================================================================
            # CHANGE 2: Standalone Assessment enrolments - built entirely
            # separately from the platform/marketplace enrolment logic below
            # (Standalone Assessments have no real enrol event - no batch, no
            # ENROLMENT_PARQUET_FILE record - so this is synthesized from
            # assessment submission data instead). Nothing else in this file
            # is touched by this block.
            # ================================================================
            print("🔄 Processing Standalone Assessment enrolments...")
            standaloneAssessRawDF = assessmentDFUtil.standalone_assessment_enrolments_dataframe(spark)

            standaloneAssessWarehouseDF = standaloneAssessRawDF \
                .withColumn("enrolled_on",
                            date_format(from_unixtime(col("enrolled_on_epoch")), ParquetFileConstants.DATE_TIME_FORMAT)) \
                .withColumn("content_last_accessed_on",
                            date_format(from_unixtime(col("last_accessed_epoch")), ParquetFileConstants.DATE_TIME_FORMAT)) \
                .withColumn("data_last_generated_on", currentDateTime) \
                .select(
                col("userID").alias("user_id"),
                col("batchID").alias("batch_id"),
                col("content_id"),
                col("enrolled_on"),
                col("content_progress_percentage").cast("decimal(17,2)"),
                lit(0).alias("resource_count_consumed"),
                col("user_consumption_status"),
                col("enrolment_status"),
                lit(None).cast(StringType()).alias("first_completed_on"),
                lit(None).cast(StringType()).alias("first_certificate_generated_on"),
                lit(None).cast(StringType()).alias("last_completed_on"),
                lit(None).cast(StringType()).alias("last_certificate_generated_on"),
                col("content_last_accessed_on"),
                col("certificate_generated"),
                lit(0).alias("number_of_certificate"),
                lit("Not Available").alias("user_rating"),
                lit("").alias("certificate_id"),
                lit(False).alias("live_cbp_plan_mandate"),
                lit(None).cast(StringType()).alias("badge_id"),
                lit(0).cast(IntegerType()).alias("karma_points"),
                col("data_last_generated_on")
            )
            # ================================================================
            # END CHANGE 2
            # ================================================================
            print("🔄 Processing platform enrolments...")
            # Compute and cache the main platform join result
            allCourseProgramCompletionWithDetailsDFWithRating = enrolmentDFUtil.preComputeUserOrgEnrolment(enrolmentDF,
                                                                                                           contentOrgDF,
                                                                                                           userOrgDF,
                                                                                                           spark)
            # Process platform data and cache the result
            df = (
                UserEnrolmentModel.duration_format(allCourseProgramCompletionWithDetailsDFWithRating, "courseDuration")
                .withColumn("badge_details", explode_outer("issued_badges"))
                .withColumn("completedOn",
                            date_format(col("courseCompletedTimestamp"), ParquetFileConstants.DATE_TIME_FORMAT))
                .withColumn("enrolledOn",
                            date_format(col("courseEnrolledTimestamp"), ParquetFileConstants.DATE_TIME_FORMAT))
                .withColumn("firstCompletedOn",
                            date_format(col("firstCompletedOn"), ParquetFileConstants.DATE_TIME_FORMAT))
                .withColumn("lastContentAccessTimestamp",
                            date_format(col("lastContentAccessTimestamp"), ParquetFileConstants.DATE_TIME_FORMAT))
                .withColumn("courseLastPublishedOn",
                            to_date(col("courseLastPublishedOn"), ParquetFileConstants.DATE_FORMAT))
                .withColumn("courseBatchStartDate",
                            to_date(col("courseBatchStartDate"), ParquetFileConstants.DATE_FORMAT))
                .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), ParquetFileConstants.DATE_FORMAT))
                .withColumn("completionPercentage", round(col("completionPercentage"), 2))
                .withColumn("Report_Last_Generated_On", currentDateTime)
                .withColumn("Certificate_Generated",
                            when(col("issuedCertificateCount") > 0, "Yes").otherwise("No"))
                .withColumn("ArchivedOn",
                            when(col("courseStatus") == "Retired", col("lastStatusChangedOn")).otherwise(""))
                .withColumn("ArchivedOn", to_date(col("ArchivedOn"), ParquetFileConstants.DATE_FORMAT))
                .withColumn("Certificate_ID", col("certificateID"))
                .dropDuplicates(["userID", "courseID", "batchID"])
            )
            print("🔄 Processing external/marketplace enrolments...")

            # Load external data
            externalEnrolmentDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE).withColumn("badge_details", explode_outer("issued_badges"))
            externalContentOrgDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)

            # Process marketplace data and cache
            marketPlaceContentEnrolmentsDF = (
                UserEnrolmentModel.duration_format(externalContentOrgDF, "courseDuration")
                .join(externalEnrolmentDF, "content_id", "inner")
                .withColumn("courseCompletedTimestamp",
                            date_format(col("completedon"), ParquetFileConstants.DATE_TIME_FORMAT))
                .withColumn("courseEnrolledTimestamp",
                            date_format(col("enrolled_date"), ParquetFileConstants.DATE_TIME_FORMAT))
                .withColumn("lastContentAccessTimestamp", lit("Not Available"))
                .withColumn("userRating", lit("Not Available"))
                .withColumn("live_cbp_plan_mandate", lit(False))
                .withColumn("batchID", lit("Not Available"))
                .withColumn("issuedCertificateCount", size(col("issued_certificates")))
                .withColumn("certificate_generated",
                            expr("CASE WHEN issuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END"))
                .withColumn("certificateGeneratedOn",
                            when(col("issued_certificates").isNull(), "")
                            .otherwise(
                                col("issued_certificates")[size(col("issued_certificates")) - 1]["lastIssuedOn"]))
                .withColumn("firstCompletedOn",
                            when(col("issued_certificates").isNull(), "")
                            .otherwise(
                                when(size(col("issued_certificates")) > 0,
                                     col("issued_certificates")[0]["lastIssuedOn"])
                                .otherwise("")))
                .withColumn("certificateID",
                            when(col("issued_certificates").isNull(), "")
                            .otherwise(col("issued_certificates")[size(col("issued_certificates")) - 1]["identifier"]))
                .withColumn("Report_Last_Generated_On", currentDateTime)
                .withColumnRenamed("userid", "userID")
                .withColumnRenamed("content_id", "courseID")
                .withColumnRenamed("progress", "courseProgress")
                .withColumnRenamed("status", "dbCompletionStatus")
                .fillna(0, subset=["courseProgress", "issuedCertificateCount"])
                .fillna("", subset=["certificateGeneratedOn"])
            )
            marketPlaceEnrolmentsWithUserDetailsDF = marketPlaceContentEnrolmentsDF.join(userOrgDF, ["userID"], "left")

            print("🔄 Processing ACBP data...")
            course_schema = StructType([
                StructField("identifier", StringType(), True),
                StructField("mandatory", BooleanType(), True),
            ])

            # Load and process ACBP data
            acbpAllEnrolmentDF = (spark.read.parquet(ParquetFileConstants.ACBP_COMPUTED_FILE)
                                  .withColumn("rawCourse", explode(col("acbpCourseIDList"))) \
                                  .withColumn("parsedCourse", from_json(col("rawCourse"), course_schema)) \
                                  .withColumn("courseID", coalesce(col("parsedCourse.identifier"), col("rawCourse"))) \
                                  .withColumn("courseID", regexp_replace(col("courseID"), r"^\s*\[|\]\s*$|\s+", "")) \
                                  .withColumn("liveCBPlan", lit(True))
                                  .select(col("userOrgID"), col("courseID"), col("userID"),
                                          col("designation"), col("liveCBPlan")))

            # Join platform data with ACBP, then with the (de-duplicated) unenrolment audit
            # data, in a single chain so neither join gets thrown away.
            enrolmentWithACBP = (
                df.join(acbpAllEnrolmentDF, ["userID", "userOrgID", "courseID"], "left")
                .withColumn("live_cbp_plan_mandate",
                            when(col("liveCBPlan").isNull(), False)
                            .otherwise(col("liveCBPlan")))
                .join(unenrolmentAuditLatestDF, ["userID", "courseID", "batchID"], "left")
                .withColumn("unenrolled_on",
                            when(col("unenrolledOn").isNotNull(),
                                 date_format(col("unenrolledOn"), ParquetFileConstants.DATE_TIME_FORMAT))
                            .otherwise(lit(None).cast(StringType())))
                .drop("action", "unenrolledOn")
            )


            print("🔄 Generating reports...")

            # Generate marketplace report
            mdoMarketplaceReport = (marketPlaceEnrolmentsWithUserDetailsDF
            .withColumn("MDO_Name", col("userOrgName"))
            .withColumn("Ministry",
                        when(col("ministry_name").isNull(), col("userOrgName"))
                        .otherwise(col("ministry_name")))
            .withColumn("Department",
                        when((col("Ministry").isNotNull()) &
                             (col("Ministry") != col("userOrgName")) &
                             ((col("dept_name").isNull()) | (col("dept_name") == "")),
                             col("userOrgName"))
                        .otherwise(col("dept_name")))
            .withColumn("Organization",
                        when((col("Ministry") != col("userOrgName")) &
                             (col("Department") != col("userOrgName")),
                             col("userOrgName"))
                        .otherwise(lit("")))
            .filter(col("userStatus").cast("int") == 1)
            .withColumn("enrolment_status", lit("enrolled"))
            .withColumn("is_enrolled",
                        when(col("dbCompletionStatus").isNull(), "unenrolled")
                        .otherwise("enrolled"))
            .withColumn("unenrolled_on", lit(None).cast(StringType()))
            .select(
                col("fullName").alias("Full_Name"),
                col("professionalDetails.designation").alias("Designation"),
                col("personalDetails.primaryEmail").alias("Email"),
                col("personalDetails.mobile").alias("Phone_Number"),
                col("MDO_Name"),
                col("professionalDetails.group").alias("Group"),
                col("Tag"),
                col("Ministry"),
                col("Department"),
                col("Organization"),
                col("employmentDetails.employeeCode").alias("Employee Id"),
                col("cadreName").alias("Cadre"),
                col("civilServiceType").alias("Civil Service Type"),
                col("civilServiceName").alias("Civil Services"),
                col("cadreBatch").alias("Cadre Batch"),
                col("organised_service").alias("Is From Organised Service of Govt"),
                col("courseOrgName").alias("Content_Provider"),
                col("courseName").alias("Content_Name"),
                col("category").alias("Content_Type"),
                col("courseDuration").alias("Content_Duration"),
                col("batchID").alias("Batch_Id"),
                lit("Not Available").alias("Batch_Name"),
                lit(None).cast(DateType()).alias("Batch_Start_Date"),
                lit(None).cast(DateType()).alias("Batch_End_Date"),
                col("courseEnrolledTimestamp").alias("Enrolled_On"),
                when(col("dbCompletionStatus").isNull(), "not-enrolled")
                .when(col("dbCompletionStatus") == 0, "not-started")
                .when(col("dbCompletionStatus") == 1, "in-progress")
                .otherwise("completed")
                .alias("Status"),
                col("completionpercentage").alias("Content_Progress_Percentage"),
                to_date(col("courseLastPublishedOn"), ParquetFileConstants.DATE_FORMAT).alias("Last_Published_On"),
                lit(None).cast(DateType()).alias("Content_Retired_On"),
                col("courseCompletedTimestamp").alias("Completed_On"),
                col("certificate_generated").alias("Certificate_Generated"),
                col("userRating").alias("User_Rating"),
                col("personalDetails.gender").alias("Gender"),
                lit("External Content").alias("Category"),
                col("additionalProperties.externalSystem").alias("External_System"),
                col("additionalProperties.externalSystemId").alias("External_System_Id"),
                col("userOrgID").alias("mdoid"),
                col("certificateID").alias("Certificate_ID"),
                col("Report_Last_Generated_On"),
                col("live_cbp_plan_mandate").alias("Live_CBP_Plan_Mandate"),
                col("is_enrolled"),
                col("enrolment_status"),
                col("unenrolled_on"),
                col("role").alias("Roles")  # used only to classify Govt / Non-Govt users, dropped before write
            )
            )

            marketPlaceWarehouseDF = (marketPlaceEnrolmentsWithUserDetailsDF
                                      .withColumn("certificate_generated_on",
                                                  date_format(
                                                      from_utc_timestamp(
                                                          to_utc_timestamp(
                                                              to_timestamp(col("certificateGeneratedOn"),
                                                                           ParquetFileConstants.DATE_TIME_WITH_MILLI_SEC_FORMAT),
                                                              "UTC"
                                                          ),
                                                          "IST"
                                                      ),
                                                      ParquetFileConstants.DATE_TIME_FORMAT
                                                  ))
                                      .withColumn("data_last_generated_on", currentDateTime)
                                      .withColumn("enrolment_status", lit("enrolled"))
                                      .withColumn("firstCompletedOn", date_format(col("firstCompletedOn"),
                                                                                  ParquetFileConstants.DATE_TIME_FORMAT))
                                      .select(
                col("userID").alias("user_id"),
                col("batchID").alias("batch_id"),
                col("courseID").alias("content_id"),
                col("courseEnrolledTimestamp").alias("enrolled_on"),
                col("completionpercentage").alias("content_progress_percentage"),
                col("courseProgress").alias("resource_count_consumed"),
                when(col("dbCompletionStatus").isNull(), "not-enrolled")
                .when(col("dbCompletionStatus") == 0, "not-started")
                .when(col("dbCompletionStatus") == 1, "in-progress")
                .otherwise("completed")
                .alias("user_consumption_status"),
                col("enrolment_status"),
                col("firstCompletedOn").alias("first_completed_on"),
                col("firstCompletedOn").alias("first_certificate_generated_on"),
                col("courseCompletedTimestamp").alias("last_completed_on"),
                col("certificate_generated_on").alias("last_certificate_generated_on"),
                col("lastContentAccessTimestamp").alias("content_last_accessed_on"),
                col("certificate_generated").alias("certificate_generated"),
                col("issuedCertificateCount").alias("number_of_certificate"),
                col("userRating").alias("user_rating"),
                col("certificateID").alias("certificate_id"),
                col("live_cbp_plan_mandate"),
                col("badge_details.badgeId").alias("badge_id"),
                col("data_last_generated_on")
            )
                                      .withColumn("karma_points", lit(0).cast(IntegerType()))
                                      .dropDuplicates(["user_id", "batch_id", "content_id"])
                                      )

            mdoPlatformReport = (enrolmentWithACBP
                                 .withColumn("MDO_Name", col("userOrgName"))
                                 .withColumn("Ministry",
                                             when(col("ministry_name").isNull(), col("userOrgName"))
                                             .otherwise(col("ministry_name")))
                                 .withColumn("Department",
                                             when((col("Ministry").isNotNull()) &
                                                  (col("Ministry") != col("userOrgName")) &
                                                  ((col("dept_name").isNull()) | (col("dept_name") == "")),
                                                  col("userOrgName"))
                                             .otherwise(col("dept_name")))
                                 .withColumn("Organization",
                                             when((col("Ministry") != col("userOrgName")) &
                                                  (col("Department") != col("userOrgName")),
                                                  col("userOrgName"))
                                             .otherwise(lit("")))
                                 .filter(col("userStatus").cast("int") == 1)
                                 .withColumn("is_enrolled",
                                             when((col("userCourseCompletionStatus").isNull()) |
                                                  (col("userCourseCompletionStatus") == "not-enrolled"),
                                                  "unenrolled")
                                             .otherwise("enrolled"))
                                 .select(
                col("fullName").alias("Full_Name"),
                col("professionalDetails.designation").alias("Designation"),
                col("personalDetails.primaryEmail").alias("Email"),
                col("personalDetails.mobile").alias("Phone_Number"),
                col("MDO_Name"),
                col("professionalDetails.group").alias("Group"),
                col("Tag"),
                col("Ministry"),
                col("Department"),
                col("Organization"),
                col("employmentDetails.employeeCode").alias("Employee Id"),
                col("cadreName").alias("Cadre"),
                col("civilServiceType").alias("Civil Service Type"),
                col("civilServiceName").alias("Civil Services"),
                col("cadreBatch").alias("Cadre Batch"),
                col("organised_service").alias("Is From Organised Service of Govt"),
                col("courseOrgName").alias("Content_Provider"),
                col("courseName").alias("Content_Name"),
                col("category").alias("Content_Type"),
                col("courseDuration").alias("Content_Duration"),
                col("batchID").alias("Batch_Id"),
                col("courseBatchName").alias("Batch_Name"),
                col("courseBatchStartDate").alias("Batch_Start_Date"),
                col("courseBatchEndDate").alias("Batch_End_Date"),
                col("enrolledOn").alias("Enrolled_On"),
                col("userCourseCompletionStatus").alias("Status"),
                col("completionPercentage").alias("Content_Progress_Percentage"),
                col("courseLastPublishedOn").alias("Last_Published_On"),
                col("ArchivedOn").alias("Content_Retired_On"),
                col("completedOn").alias("Completed_On"),
                col("Certificate_Generated"),
                col("userRating").alias("User_Rating"),
                col("personalDetails.gender").alias("Gender"),
                col("personalDetails.category").alias("Category"),
                col("additionalProperties.externalSystem").alias("External_System"),
                col("additionalProperties.externalSystemId").alias("External_System_Id"),
                col("userOrgID").alias("mdoid"),
                col("Certificate_ID"),
                col("Report_Last_Generated_On"),
                col("live_cbp_plan_mandate").alias("Live_CBP_Plan_Mandate"),
                col("userID"),
                col("courseID"),
                col("is_enrolled"),
                col("enrolment_status"),
                when(col("enrolment_status") == "unenrolled", col("unenrolled_on")).otherwise(None).alias("unenrolled_on"),
                col("role").alias("Roles")  # used only to classify Govt / Non-Govt users, dropped before write
            )
                                 .dropDuplicates(["userID", "Batch_Id", "courseID"])
                                 .drop("userID", "courseID")
                                 )

            platformWarehouseDF = (enrolmentWithACBP
                                   .withColumn("certificate_generated_on",
                                               date_format(
                                                   from_utc_timestamp(
                                                       to_utc_timestamp(
                                                           to_timestamp(col("certificateGeneratedOn"),
                                                                        ParquetFileConstants.DATE_TIME_WITH_MILLI_SEC_FORMAT),
                                                           "UTC"
                                                       ),
                                                       "IST"
                                                   ),
                                                   ParquetFileConstants.DATE_TIME_FORMAT
                                               ))
                                   .withColumn("data_last_generated_on", currentDateTime)
                                   .select(
                col("userID").alias("user_id"),
                col("batchID").alias("batch_id"),
                col("courseID").alias("content_id"),
                col("enrolledOn").alias("enrolled_on"),
                col("completionPercentage").alias("content_progress_percentage"),
                col("courseProgress").alias("resource_count_consumed"),
                col("userCourseCompletionStatus").alias("user_consumption_status"),
                col("enrolment_status"),
                col("firstCompletedOn").alias("first_completed_on"),
                col("firstCompletedOn").alias("first_certificate_generated_on"),
                col("completedOn").alias("last_completed_on"),
                col("certificate_generated_on").alias("last_certificate_generated_on"),
                col("lastContentAccessTimestamp").alias("content_last_accessed_on"),
                col("Certificate_Generated").alias("certificate_generated"),
                col("issuedCertificateCount").alias("number_of_certificate"),
                col("userRating").alias("user_rating"),
                col("Certificate_ID").alias("certificate_id"),
                col("live_cbp_plan_mandate"),
                col("data_last_generated_on"),
                col("karma_points"),
                col("badge_details")["badgeId"].alias("badge_id"),
            )
                                   .fillna(0, subset=["karma_points"])
                                   .dropDuplicates(["user_id", "batch_id", "content_id"])
                                   )

            print("🔄 Combining and writing final outputs...")

            mdoReportDF = (
                mdoPlatformReport
                .union(mdoMarketplaceReport)
            )

            platform_enrolments_df = mdoPlatformReport
            marketplace_enrolments_df = mdoMarketplaceReport

            # ---- Govt / Non-Govt classification ----
            # Exact (trimmed, case-insensitive) match on Designation, and an exact
            # match against any element of the Roles array. Using .contains(...)
            # here would wrongly flag designations like "Civil Defence Volunteer"
            # as Non-Govt just because the substring "VOLUNTEER" appears in them.
            mdoReportDF = mdoReportDF.withColumn(
                "is_non_govt_user",
                when(
                    (trim(upper(coalesce(col("Designation"), lit("")))) == "VOLUNTEER") |
                    coalesce(exists(split(coalesce(col("Roles"), lit("")), ","), lambda r: trim(upper(r)) == "VOLUNTEER"), lit(False)),
                    lit(True)
                ).otherwise(lit(False))
            ).cache()

            govt_part_df = mdoReportDF.filter(~col("is_non_govt_user")).drop("is_non_govt_user", "Roles")
            non_govt_part_df = mdoReportDF.filter(col("is_non_govt_user")).drop("is_non_govt_user", "Roles")

            # Determine, per org, whether it has govt users, non-govt users, or both.
            govt_org_ids = set(
                row.mdoid for row in govt_part_df.select("mdoid").distinct().collect() if row.mdoid is not None
            )
            non_govt_org_ids = set(
                row.mdoid for row in non_govt_part_df.select("mdoid").distinct().collect() if row.mdoid is not None
            )
            both_org_ids = govt_org_ids & non_govt_org_ids

            print(f"Orgs with only Govt users: {len(govt_org_ids - both_org_ids)}")
            print(f"Orgs with only Non-Govt users: {len(non_govt_org_ids - both_org_ids)}")
            print(f"Orgs with BOTH Govt and Non-Govt users (2 reports each): {len(both_org_ids)}")

            # Govt subset keeps its mdoid unchanged (mdoid=<org_id>).
            # Non-Govt subset keeps its mdoid unchanged too UNLESS the same org also has
            # Govt users, in which case its folder becomes mdoid=<org_id>_non_govt so the
            # two reports for that org don't collide.
            both_org_ids_list = list(both_org_ids)
            non_govt_part_df = non_govt_part_df.withColumn(
                "mdoid",
                when(col("mdoid").isin(both_org_ids_list), concat(col("mdoid"), lit("_non_govt")))
                .otherwise(col("mdoid"))
            )

            enrolment_out_path = f"{config.localReportDir}/{config.userEnrolmentReportPath}/{today}"

            print("📝 Writing CSV reports...")

            # dfexportutil.write_csv_per_mdo_id_duckdb throws a KeyError on
            # 'successful_conversions' when it's handed a dataframe with zero
            # orgs/partitions to write (e.g. no Non-Govt/VOLUNTEER users found
            # in this run at all). Guard each write so an empty subset doesn't
            # crash the whole job.
            if govt_org_ids:
                print(f"➡️  Writing GOVT user enrolment report -> {enrolment_out_path}")
                dfexportutil.write_csv_per_mdo_id_duckdb(
                    govt_part_df,
                    enrolment_out_path,
                    'mdoid',
                    f"{config.localReportDir}/temp/user_enrolment_report/{today}",
                    csv_filename=config.userEnrollmentReport
                )
            else:
                print("ℹ️  No Govt users found in this run — skipping Govt CSV write.")

            if non_govt_org_ids:
                print(f"➡️  Writing NON-GOVT user enrolment report -> {enrolment_out_path}")
                dfexportutil.write_csv_per_mdo_id_duckdb(
                    non_govt_part_df,
                    enrolment_out_path,
                    'mdoid',
                    f"{config.localReportDir}/temp/user_enrolment_report_non_govt/{today}",
                    csv_filename=config.userEnrollmentReport
                )
            else:
                print("ℹ️  No Non-Govt (VOLUNTEER) users found in this run — skipping Non-Govt CSV write.")

            print("📦 Writing warehouse data...")
            warehouseDF = platformWarehouseDF.unionByName(marketPlaceWarehouseDF).unionByName(standaloneAssessWarehouseDF)
            warehouseDF = warehouseDF.withColumn("certificate_id",when(col("certificate_id") == "", lit(None)).otherwise(col("certificate_id")))
            warehouseDF.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
                f"{config.warehouseReportDir}/{config.dwEnrollmentsTable}")

            mdoReportDF.unpersist()

            print("✅ Processing completed successfully!")

        except Exception as e:
            print(f"❌ Error occurred during UserEnrolmentModel processing: {str(e)}")
            raise e
            sys.exit(1)


def main():
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("User Enrolment Report Model - Cached") \
        .config("spark.master", "local[16]") \
        .config("spark.sql.shuffle.partitions", "240") \
        .config("spark.executor.memory", "30g") \
        .config("spark.driver.memory", "128g") \
        .config("spark.driver.memoryOverhead", "20g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    # Create model instance

    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] UserEnrolmentModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = UserEnrolmentModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] UserEnrolmentModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()


# Example usage:
if __name__ == "__main__":
    main()