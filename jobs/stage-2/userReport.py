import findspark

findspark.init()
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, lit,
    current_timestamp, date_format, from_unixtime, concat_ws, from_json, explode, trim, length, first, expr
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import collect_set
from pyspark.sql.types import ArrayType
import os
import time
from datetime import datetime

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Import reusable utilities from project
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from util import schemas
from dfutil.dfexport import dfexportutil
from jobs.default_config import create_config
from jobs.config import get_environment_config

# Initialize Spark
spark = SparkSession.builder \
    .appName("UserReportGenerator") \
    .config("spark.executor.memory", "25g") \
    .config("spark.driver.memory", "15g") \
    .config("spark.sql.caseSensitive", "true") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

print("✅ Spark Session initialized")


def processUserReport(config):
    """
    User Report Generation with minimal traceable steps
    """

    try:
        start_time = time.time()
        today = datetime.now().strftime("%Y-%m-%d")
        currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)

        # Step 1: Load User Master Data
        print("📊 Step 1: Loading User Master Data...")
        user_master_df = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
        print("✅ Step 1 Complete")

        # Step 2: Load Enrolment Data
        print("📚 Step 2: Loading Enrolment Data...")
        user_enrolment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE)

        user_badges = (spark.read.parquet(ParquetFileConstants.GAMIFICATION_BADGE_USER_ENROLMENT_PARQUET_FILE)
                       .select(col("userID").alias("user_id"), "badge_id")
                       .groupBy("user_id").agg(expr("count(distinct badge_id)").alias("total_badges_earned")))
        print("✅ Step 2 Complete")

        # Step 3: Load Content Duration
        print("📖 Step 3: Loading Content Duration Data...")
        content_duration_df = (
            spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)
            .filter((col("courseCategory") == "Course") & (col("courseCategory") == "Moderated Course"))
            .select(
                col("courseID").alias("content_id"),
                col("courseDuration").cast("double"),
                col("category")
            )
        )
        external_content_duration_df = (
            spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)
            .filter(col("category") == "External Content")
            .select(
                col("content_id"),
                col("courseDuration").alias("external_courseDuration").cast("double"),
                col("category").alias("external_category")
            )
        )
        print("✅ Step 3 Complete")

        # Step 4: Add User Status Classification
        print("🏷️ Step 4: Classifying User Status...")
        print("✅ Step 4 Complete")

        # Step 5: Join User and Content Data
        print("🔗 Step 5: Joining User and Content Data...")
        user_enrolment_master_df = userDFUtil.appendContentDurationCompletionForEachUser(
            spark, user_master_df, user_enrolment_df, external_content_duration_df, content_duration_df
        )
        print("✅ Step 5 Complete")
        

        # Step 6: Add Event Metrics
        print("📊 Step 6: Adding Event Metrics...")
        user_complete_data = userDFUtil.appendEventDurationCompletionForEachUser(
            spark, user_enrolment_master_df
        )
        print("✅ Step 6 Complete")

        # Step 7: Create Derived Columns
        print("✨ Step 7: Creating Derived Columns...")
        user_complete_data = user_complete_data \
            .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag"))) \
            .withColumn("Total_Learning_Hours",
                        coalesce(col("total_event_learning_hours_with_certificates"), lit(0)) +
                        coalesce(col("total_content_duration"), lit(0)) +
                        coalesce(col("total_external_content_duration"), lit(0))
                        ) \
            .withColumn("weekly_claps_day_before_yesterday",
                        when(col("weekly_claps_day_before_yesterday").isNull() |
                             (col("weekly_claps_day_before_yesterday") == ""),
                             lit(0)).otherwise(col("weekly_claps_day_before_yesterday")))
        print("✅ Step 7 Complete")

        # Step 8: Final Column Selection
        print("🎯 Step 8: Final Column Selection...")

        mdoWiseReportDF = user_complete_data.filter(col("userStatus").cast("int") == 1) \
            .withColumn("Report_Last_Generated_On", currentDateTime) \
            .withColumn("Total_Enrolments",
                        coalesce(col("total_event_enrolments"), lit(0)) +
                        coalesce(col("total_content_enrolments"), lit(0))) \
            .withColumn("Total_Completions",
                        coalesce(col("total_event_completions"), lit(0)) +
                        coalesce(col("total_content_completions"), lit(0))) \
            .withColumn("MDO_Name", col("userOrgName")) \
            .withColumn("Ministry",
                        when(col("ministry_name").isNull(), col("userOrgName"))
                        .otherwise(col("ministry_name"))) \
            .withColumn("Department",
                        when((col("Ministry").isNotNull()) &
                             (col("Ministry") != col("userOrgName")) &
                             ((col("dept_name").isNull()) | (col("dept_name") == "")),
                             col("userOrgName"))
                        .otherwise(col("dept_name"))) \
            .withColumn("Organization",
                        when((col("Ministry") != col("userOrgName")) &
                             (col("Department") != col("userOrgName")),
                             col("userOrgName"))
                        .otherwise(lit(""))) \
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
            from_unixtime(col("userCreatedTimestamp") / 1000, ParquetFileConstants.DATE_FORMAT).alias(
                "User_Registration_Date"),
            col("cadreName").alias("Cadre"),
            col("civilServiceType").alias("Civil Service Type"),
            col("civilServiceName").alias("Civil Services"),
            col("cadreBatch").alias("Cadre Batch"),
            col("isOnCentralDeputation").alias("Is On Central Deputation"),
            col("organised_service").alias("Is From Organised Service of Govt"),
            col("role").alias("Roles"),
            col("personalDetails.gender").alias("Gender"),
            col("personalDetails.category").alias("Category"),
            col("additionalProperties.externalSystem").alias("External_System"),
            col("additionalProperties.externalSystemId").alias("External_System_Id"),
            col("employmentDetails.employeeCode").alias("Employee_Id"),
            from_unixtime(col("userOrgCreatedDate") / 1000, ParquetFileConstants.DATE_FORMAT).alias("MDO_Created_On"),
            col("userProfileStatus").alias("Profile_Status"),
            col("weekly_claps_day_before_yesterday"),
            coalesce(col("total_points"), lit(0)).alias("Karma_Points"),
            coalesce(col("total_event_enrolments"), lit(0)).alias("Event_Enrolments"),
            coalesce(col("total_event_completions"), lit(0)).alias("Event_Completions"),
            coalesce(col("total_event_learning_hours_with_certificates"), lit(0)).alias("Event_Learning_Hours"),
            coalesce(col("total_content_enrolments"), lit(0)).alias("Course_Enrolments"),
            coalesce(col("total_content_completions"), lit(0)).alias("Course_Completions"),
            coalesce(col("total_content_duration"), lit(0)).alias("Course_Learning_Hours"),
            coalesce(col("Total_Enrolments"), lit(0)).alias("Total_Enrolments"),
            coalesce(col("Total_Completions"), lit(0)).alias("Total_Completions"),
            coalesce(col("Total_Learning_Hours"), lit(0)).alias("Total_Learning_Hours"),
            col("Report_Last_Generated_On"),
            col("userOrgID").alias("mdoid")
        )
        dfexportutil.write_csv_per_mdo_id(mdoWiseReportDF, f"{config.localReportDir}/{config.userReportPath}/{today}",
                                          'mdoid', csv_filename=config.userReport)

        warehouseDF = user_complete_data \
            .withColumn("marked_as_not_my_user",
                        when(col("userProfileStatus") == "NOT-MY-USER", lit(True)).otherwise(lit(False))) \
            .withColumn("data_last_generated_on", currentDateTime) \
            .withColumn("is_verified_karmayogi",
                        when(col("userProfileStatus") == "VERIFIED", lit(True)).otherwise(lit(False))) \
            .select(
            col("userID").alias("user_id"),
            col("userOrgID").alias("mdo_id"),
            col("userStatus").alias("status"),
            coalesce(col("total_points"), lit(0)).alias("no_of_karma_points"),
            col("fullName").alias("full_name"),
            col("professionalDetails.designation").alias("designation"),
            col("personalDetails.primaryEmail").alias("email"),
            col("personalDetails.mobile").alias("phone_number"),
            col("personalDetails.pincode").alias("pincode"),
            col("professionalDetails.group").alias("groups"),
            col("Tag").alias("tag"),
            col("userProfileStatus").alias("profile_status"),
            date_format(from_unixtime(col("userCreatedTimestamp") / 1000), ParquetFileConstants.DATE_TIME_FORMAT).alias(
                "user_registration_date"),
            date_format(from_unixtime(col("userUpdatedTimestamp") / 1000), ParquetFileConstants.DATE_TIME_FORMAT).alias(
                "profile_last_updated_date"),
            col("role").alias("roles"),
            col("personalDetails.gender").alias("gender"),
            col("personalDetails.category").alias("category"),
            col("marked_as_not_my_user"),
            col("is_verified_karmayogi"),
            col("userCreatedBy").alias("created_by_id"),
            col("additionalProperties.externalSystem").alias("external_system"),
            col("additionalProperties.externalSystemId").alias("external_system_id"),
            col("weekly_claps_day_before_yesterday"),
            coalesce(col("total_event_learning_hours_with_certificates"), lit(0)).alias("total_event_learning_hours"),
            coalesce(col("total_content_duration"), lit(0)).alias("total_content_learning_hours"),
            coalesce(col("Total_Learning_Hours"), lit(0)).alias("total_learning_hours"),
            col("employmentDetails.employeeCode").alias("employee_id"),
            col("cadreName").alias("cadre"),
            col("civilServiceType").alias("civil_service_type"),
            col("civilServiceName").alias("civil_services"),
            col("cadreBatch").alias("cadre_batch"),
            col("isOnCentralDeputation").alias("is_on_central_deputation"),
            col("organised_service").alias("is_from_organised_service_of_govt"),
            col("data_last_generated_on")
        ).join(user_badges, on="user_id", how="left").fillna(0, subset=["total_badges_earned"])
        print("✅ Step 8 Complete")
        # Step 9: Export Data
        print("📁 Step 9: Exporting Warehouse Data...")
        warehouseDF.write.mode("overwrite").option("compression", "snappy").parquet(
            f"{config.warehouseReportDir}/{config.dwUserTable}")
        print("✅ Step 9 Complete")


        # Step 10: Process User Extended Profile Data
        print("🔍 Step 10: Processing User Extended Profile Data...")
        # Load user extended profile data
        user_extended_profile_df = (
        spark.read.parquet(ParquetFileConstants.USER_EXTENDED_PROFILE)
        .filter(col("contexttype") == "orgAdditionalProperties")
        .withColumnRenamed("userid", "userID")
        .withColumn("contextDataArray", from_json(col("contextdata"), ArrayType(schemas.context_data_schema)))
        .withColumn("contextData", explode(col("contextDataArray")))
        .select(
            col("userID"),
            col("contexttype").alias("contextType"),
            col("contextData"),
            col("contextData.organisationId").alias("mdo_id")
        )
        )
        
        # Step 1: Explode customFieldValues and handle based on type
        exploded_df_base = (
            user_extended_profile_df
            .withColumn("customField", explode(col("contextData.customFieldValues")))
            .select(
                col("userID"),
                col("mdo_id"),
                col("customField.type").alias("field_type"),
                col("customField.attributeName").alias("attribute_name"),
                col("customField.value").alias("direct_value"),
                col("customField.values").alias("values_array")
            )
        )
        
        # Handle direct values (where type is not "masterList" and direct_value is not null)
        direct_values_df = (
            exploded_df_base
            .filter(
                (col("field_type") != "masterList") &
                col("direct_value").isNotNull()
            )
            .select(
                col("userID"),
                col("mdo_id"),
                col("attribute_name"),  # Use main attributeName for text fields
                col("direct_value").alias("attribute_value")
            )
        )
        
        # Handle masterList values (where type is "masterList" and values_array is not null)
        master_list_values_df = (
            exploded_df_base
            .filter(
                (col("field_type") == "masterList") &
                col("values_array").isNotNull()
            )
            .withColumn("valueItem", explode(col("values_array")))
            .select(
                col("userID"),
                col("mdo_id"),
                col("valueItem.attributeName").alias("attribute_name"),  # Use nested attributeName for masterList
                col("valueItem.value").alias("attribute_value")
            )
        )
        
        # Combine both direct values and masterList values
        exploded_df = direct_values_df.union(master_list_values_df).filter(
            col("attribute_name").isNotNull() &
            col("attribute_value").isNotNull()
        )
        
        # Write to warehouse tables
        exploded_df \
            .select(
                col("userID").alias("user_id"),
                col("mdo_id"),
                col("attribute_name"),
                col("attribute_value")
            ) \
            .coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(
            f"{config.warehouseReportDir}/userCustomFields"
        )
        
        # Cache the exploded data for reuse
        exploded_cached = exploded_df.cache()
        print("✅ Step 10 Complete")
        
        # Step 11: Create MDO-wise Slim Data for Custom Reports
        print("📋 Step 11: Creating MDO-wise Custom Reports...")
        
        mdo_wise_slim = (
            user_complete_data
            .filter(col("userStatus").cast("int") == 1)
            .select(
                col("userID"),
                col("fullName").alias("Full_Name"),
                col("professionalDetails.designation").alias("Designation"),
                col("personalDetails.primaryEmail").alias("Email"),
                col("personalDetails.mobile").alias("Phone_Number"),
                col("userOrgName").alias("MDO_Name"),
                col("professionalDetails.group").alias("Group"),
                col("Tag"),
                when(col("ministry_name").isNull(), col("userOrgName"))
                    .otherwise(col("ministry_name")).alias("Ministry"),
                when(
                    (col("ministry_name").isNotNull()) &
                    (col("ministry_name") != col("userOrgName")) &
                    ((col("dept_name").isNull()) | (col("dept_name") == "")),
                    col("userOrgName")
                ).otherwise(col("dept_name")).alias("Department"),
                when(
                    (col("ministry_name") != col("userOrgName")) &
                    (col("dept_name") != col("userOrgName")),
                    col("userOrgName")
                ).otherwise(lit("")).alias("Organization"),
                from_unixtime(col("userCreatedTimestamp") / 1000, ParquetFileConstants.DATE_FORMAT).alias("User_Registration_Date"),
                col("role").alias("Roles"),
                col("personalDetails.gender").alias("Gender"),
                col("personalDetails.category").alias("Category"),
                col("additionalProperties.externalSystem").alias("External_System"),
                col("additionalProperties.externalSystemId").alias("External_System_Id"),
                col("employmentDetails.employeeCode").alias("Employee_Id"),
                from_unixtime(col("userOrgCreatedDate") / 1000, ParquetFileConstants.DATE_FORMAT).alias("MDO_Created_On"),
                col("userProfileStatus").alias("Profile_Status"),
                col("weekly_claps_day_before_yesterday"),
                coalesce(col("total_points"), lit(0)).alias("Karma_Points"),
                coalesce(col("total_event_enrolments"), lit(0)).alias("Event_Enrolments"),
                coalesce(col("total_event_completions"), lit(0)).alias("Event_Completions"),
                coalesce(col("total_event_learning_hours_with_certificates"), lit(0)).alias("Event_Learning_Hours"),
                coalesce(col("total_content_enrolments"), lit(0)).alias("Course_Enrolments"),
                coalesce(col("total_content_completions"), lit(0)).alias("Course_Completions"),
                coalesce(col("total_content_duration"), lit(0)).alias("Course_Learning_Hours"),
                (coalesce(col("total_event_enrolments"), lit(0)) +
                coalesce(col("total_content_enrolments"), lit(0))).alias("Total_Enrolments"),
                (coalesce(col("total_event_completions"), lit(0)) +
                coalesce(col("total_content_completions"), lit(0))).alias("Total_Completions"),
                coalesce(col("Total_Learning_Hours"), lit(0)).alias("Total_Learning_Hours"),
                lit(currentDateTime).alias("Report_Last_Generated_On"),
                col("userOrgID").alias("mdoid")
            )
        ).repartition(col("mdoid")).cache()  # Repartition by org and cache

        base_out = f"standalone-reports/user-custom-report/{today}"

        print("📊 Pre-collecting organization metadata...")
        org_metadata = (
            exploded_cached
            .groupBy("mdo_id")
            .agg(collect_set("attribute_name").alias("custom_fields"))
            .collect()
        )

        org_custom_fields = {
            row.mdo_id: sorted([field for field in row.custom_fields if field and field.strip()])
            for row in org_metadata
        }

        org_ids = sorted(org_custom_fields.keys())
        print(f"Found {len(org_ids)} organizations with custom fields")

        fixed_cols = [
            "userID", "Full_Name", "Designation", "Email", "Phone_Number", "MDO_Name", "Group", "Tag",
            "Ministry", "Department", "Organization", "User_Registration_Date", "Roles", "Gender",
            "Category", "External_System", "External_System_Id", "Employee_Id", "MDO_Created_On",
            "Profile_Status", "weekly_claps_day_before_yesterday", "Karma_Points", "Event_Enrolments",
            "Event_Completions", "Event_Learning_Hours", "Course_Enrolments", "Course_Completions",
            "Course_Learning_Hours", "Total_Enrolments", "Total_Completions", "Total_Learning_Hours",
            "Report_Last_Generated_On", "mdoid"
        ]
        fixed_cols_lower = [col_name.lower() for col_name in fixed_cols]

        def process_single_organization(org_id):
            """Process a single organization - same logic as before but with fixed column handling"""
            try:
                print(f"  Processing organization: {org_id}")
                
                # Get pre-computed custom fields for this org
                attribute_names = org_custom_fields.get(org_id, [])
                
                # Filter data for current organization
                org_data = exploded_cached.filter(col("mdo_id") == org_id)
                
                # Create pivot - same logic as before
                if attribute_names:
                    pivoted = (
                        org_data
                        .groupBy("userID")
                        .pivot("attribute_name")
                        .agg(first("attribute_value"))
                    )
                else:
                    pivoted = org_data.select("userID").distinct()
                
                # Handle conflicts - same logic as before
                conflicts = []
                attribute_names_lower = [attr_name.lower() for attr_name in attribute_names]
                
                for i, attr_lower in enumerate(attribute_names_lower):
                    if attr_lower in fixed_cols_lower:
                        conflicts.append(attribute_names[i])
                 
                # Rename conflicting columns
                renamed_pivoted = pivoted
                custom_field_mapping = {}
                
                for conflict_col in conflicts:
                    if conflict_col in pivoted.columns:
                        new_name = f"Custom_{conflict_col}"
                        renamed_pivoted = renamed_pivoted.withColumnRenamed(conflict_col, new_name)
                        custom_field_mapping[conflict_col] = new_name
                
                # Join with user data - same logic as before
                org_user_data = mdo_wise_slim.filter(col("mdoid") == org_id)
                joined = (
                    renamed_pivoted
                    .join(org_user_data, ["userID"], "left")
                    .withColumn("mdoid", lit(org_id))
                )
                
                # Create final column list - same logic as before
                final_custom_cols = []
                for attr_name in attribute_names:
                    if attr_name in custom_field_mapping:
                        final_custom_cols.append(custom_field_mapping[attr_name])
                    else:
                        final_custom_cols.append(attr_name)
                
                available_columns = set(joined.columns)
                existing_fixed_cols = [c for c in fixed_cols if c in available_columns]
                existing_custom_cols = [c for c in final_custom_cols if c in available_columns]
                final_columns = existing_fixed_cols + existing_custom_cols
                
                # Remove duplicates
                final_columns = list(dict.fromkeys(final_columns))
                
                def safe_column_reference(col_name):
                    """Create safe column reference for selection"""
                    # If column name has special characters, use backticks in selectExpr
                    if any(char in col_name for char in ['.', ' ', '(', ')', '-', '/', '`']):
                        return f"`{col_name}`"
                    else:
                        return col_name
                
                # Create selectExpr list instead of using col() function
                select_expressions = [safe_column_reference(c) for c in final_columns]
                
                ordered = joined.selectExpr(*select_expressions)
                
                out_path = f"{config.localReportDir}/{base_out}/mdoid={org_id}"
                csv_file_path = f"{out_path}/UserCustomReport.csv"
                
                os.makedirs(out_path, exist_ok=True)
                
                result = dfexportutil.write_single_csv_duckdb(
                    df=ordered,
                    output_path=csv_file_path,
                    parquet_tmp_path=f"{out_path}/temp_parquet_{org_id}",
                    keep_parquets=False
                )

                # write warehouse files - 'warehouseUserCustomReportDir': 'user_custom_report'
                warehouse_user_custom_report_file = f"{config.warehouseReportDir}/{config.warehouseUserCustomReportDir}/{org_id}_custom_report.parquet"
                
                # Write as Parquet
                dfexportutil.write_single_parquet(
                    df=ordered,
                    final_path=warehouse_user_custom_report_file)
                
                return {
                    'org_id': org_id,
                    'success': result.get('success', True),
                    'rows_written': result.get('rows_written', 0),
                    'custom_fields_count': len(attribute_names),
                    'error': None
                }
                
            except Exception as e:
                return {
                    'org_id': org_id,
                    'success': False,
                    'rows_written': 0,
                    'custom_fields_count': len(attribute_names) if 'attribute_names' in locals() else 0,
                    'error': str(e)
                }

        max_workers = min(8, len(org_ids))
        print(f"Processing {len(org_ids)} organizations using {max_workers} parallel workers...")

        successful_orgs = 0
        failed_orgs = 0
        total_rows = 0
        start_time = time.time()

        if max_workers > 1 and len(org_ids) > 3:
            # Parallel processing for multiple organizations
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_org = {executor.submit(process_single_organization, org_id): org_id
                                for org_id in org_ids}
                
                # Collect results as they complete
                for future in as_completed(future_to_org):
                    result = future.result()
                    
                    if result['success']:
                        successful_orgs += 1
                        total_rows += result['rows_written']
                        print(f"  ✅ {result['org_id']}: {result['rows_written']:,} rows, {result['custom_fields_count']} custom fields")
                    else:
                        failed_orgs += 1
                        print(f"  ❌ {result['org_id']}: {result['error']}")

        else:
            # Sequential processing for small number of organizations
            for org_id in org_ids:
                result = process_single_organization(org_id)
                
                if result['success']:
                    successful_orgs += 1
                    total_rows += result['rows_written']
                    print(f"  ✅ {result['org_id']}: {result['rows_written']:,} rows, {result['custom_fields_count']} custom fields")
                else:
                    failed_orgs += 1
                    print(f"  ❌ {result['org_id']}: {result['error']}")


        # all_org_ids = [row.mdoid for row in mdo_wise_slim.select("mdoid").distinct().collect()]
        # orgs_without_custom = [org_id for org_id in all_org_ids if org_id not in org_ids]

        # if orgs_without_custom:
        #     print(f"\n📊 Processing {len(orgs_without_custom)} organizations without custom fields...")
            
        #     # Use bulk processing for organizations without custom fields
        #     standard_orgs_df = mdo_wise_slim.filter(col("mdoid").isin(orgs_without_custom))
            
        #     bulk_result = dfexportutil.write_csv_per_mdo_id_duckdb(
        #         df=standard_orgs_df,
        #         output_dir=f"{config.localReportDir}/{base_out}",
        #         group_by_attr="mdoid",
        #         parquet_tmp_path=f"{config.localReportDir}/{base_out}_standard_orgs",
        #         large_ids=orgs_without_custom,
        #         max_workers=max_workers,
        #         keep_parquets=False,
        #         csv_filename="UserCustomReport.csv"
        #     )
            
        #     successful_orgs += bulk_result.get('successful_writes', 0)
        #     failed_orgs += bulk_result.get('failed_writes', 0)
        #     print(f"✅ Standard organizations: {bulk_result.get('successful_writes', 0)} successful")

        exploded_cached.unpersist()
        mdo_wise_slim.unpersist()

    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise


def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] UserReport processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    processUserReport(config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] UserReport completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()