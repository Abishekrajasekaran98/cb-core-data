import sys
from pathlib import Path
from typing import List

from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql import SparkSession, DataFrame,functions as F
from pyspark.sql.functions import (
    col, lit, element_at, size, when, coalesce,expr,sum,date_format
)
from pyspark.sql.types import IntegerType


sys.path.append(str(Path(__file__).resolve().parents[2]))
from constants.ParquetFileConstants import ParquetFileConstants

def preComputeEnrolment(
    spark: SparkSession,
    extra_cols: List[str] = None,
    dates_as_long: bool = False
) -> DataFrame:
    if extra_cols is None:
        extra_cols = []
    
    # Define base columns - using list for better performance
    base_cols = [
        "userID", "courseID", "batchID", "courseProgress","courseContentStatus", 
        "dbCompletionStatus", "courseCompletedTimestamp",
        "courseEnrolledTimestamp", "lastContentAccessTimestamp", 
        "issuedCertificateCount", "issuedCertificateCountPerContent", 
        "firstCompletedOn", "certificateGeneratedOn", "certificateID", "langCourseContentStatus", "issued_badges", "enrolment_status"
    ]
    
    select_cols = base_cols + extra_cols
        
    enrolmentRawDF=spark.read.parquet(ParquetFileConstants.ENROLMENT_PARQUET_FILE)
    # all records
    # when active is true then user is enrolled else user has unenrolled
    enrolmentDF=enrolmentRawDF.withColumn("courseCompletedTimestamp", col("completedon")) \
        .withColumn("courseEnrolledTimestamp", col("enrolled_date")) \
        .withColumn("lastContentAccessTimestamp", col("lastcontentaccesstime")) \
        .withColumn("cert_array_size", coalesce(size(col("issued_certificates")), lit(0))) \
        .withColumn("issuedCertificateCount", col("cert_array_size")) \
        .withColumn(
            "issuedCertificateCountPerContent",
            when(col("cert_array_size") > 0, lit(1)).otherwise(lit(0)),
        ) \
        .withColumn(
            "certificateGeneratedOn",
            when(
                col("cert_array_size") > 0,
                element_at(col("issued_certificates"), -1)["lastIssuedOn"],
            ).otherwise(lit("")),
        ) \
        .withColumn(
            "firstCompletedOn",
            when(
                col("cert_array_size") > 0,
                element_at(col("issued_certificates"), 1)["lastIssuedOn"],
            ).otherwise(lit("")),
        ) \
        .withColumn(
            "certificateID",
            when(
                col("cert_array_size") > 0,
                element_at(col("issued_certificates"), -1)["identifier"],
            ).otherwise(lit("")),
        ) \
        .withColumn(
            "enrolment_status",
            when(
                col("active").isNotNull() & (col("active") == "true"),
                lit("enrolled"),
            ).when(
                col("active").isNotNull() & (col("active") == "false"),
                lit("unenrolled"),
            ).otherwise(None)
        ) \
        .withColumnRenamed("userid", "userID") \
        .withColumnRenamed("courseid", "courseID") \
        .withColumnRenamed("batchid", "batchID") \
        .withColumnRenamed("progress", "courseProgress") \
        .withColumnRenamed("status", "dbCompletionStatus") \
        .withColumnRenamed("contentstatus", "courseContentStatus") \
        .withColumnRenamed("lang_contentstatus", "langCourseContentStatus") \
        .withColumn("issued_badges", col("issued_badges")) \
        .fillna(
            {
                "courseProgress": 0,
                "issuedCertificateCount": 0,
                "certificateGeneratedOn": "",
                "firstCompletedOn": "",
                "certificateID": "",
            }
        ) \
        .drop("cert_array_size") \
        .select(*select_cols)
    
    if dates_as_long:
        date_columns = [
            "courseCompletedTimestamp", 
            "courseEnrolledTimestamp", 
            "lastContentAccessTimestamp"
        ]
        
        for date_col in date_columns:
            enrolmentDF = enrolmentDF.withColumn(date_col, col(date_col).cast("long"))

    # parquet file for enrolled and unenrolled records
    #unenrolledDF = enrolmentDF.filter(col("enrolment_status") == "unenrolled")
    #exportDFToParquet(unenrolledDF,ParquetFileConstants.ENROLMENT_UNENROLLED_PARQUET_FILE)
    #enrolledDF = enrolmentDF.filter(col("enrolment_status") == "enrolled")
    exportDFToParquet(enrolmentDF,ParquetFileConstants.ENROLMENT_SELECT_PARQUET_FILE)
    

    batchDF= spark.read.parquet(ParquetFileConstants.BATCH_PARQUET_FILE) \
            .select(
                col("courseid").alias("courseID"),
                col("batchid").alias("batchID"),
                col("name").alias("courseBatchName"),
                col("createdby").alias("courseBatchCreatedBy"),
                col("start_date").alias("courseBatchStartDate"),
                col("end_date").alias("courseBatchEndDate"),
                col("batch_attributes").alias("courseBatchAttrs")
            ) \
            .fillna("{}", subset=["courseBatchAttrs"])
    
    exportDFToParquet(batchDF,ParquetFileConstants.BATCH_SELECT_PARQUET_FILE)

    
    enrolmentDF = enrolmentDF.join(
    batchDF,
    on=["courseID", "batchID"],
    how="left"
)

    userRatingDF= spark.read.parquet(ParquetFileConstants.RATING_COMPUTED_PARQUET_FILE)
    enrolmentUserBatchRatingDF=enrolmentDF.join(userRatingDF, on=["userID", "courseID"], how="left") 

    userKarmaPointsRawDF = spark.read.parquet(ParquetFileConstants.USER_KARMA_POINTS_PARQUET_FILE).select(
                            col("userid").alias("userID"),
                            col("context_id").alias("courseID"),
                            col("points")
                        )
    userKarmaPointsDF = userKarmaPointsRawDF \
    .groupBy("userID", "courseID") \
    .agg(sum(coalesce(col("points").cast("double"), lit(0.0))).alias("karma_points"))
                        
    enrolmentUserBatchRatingKarmaDF=enrolmentUserBatchRatingDF.join(userKarmaPointsDF, on=["userID", "courseID"], how="left") 

    exportDFToParquet(enrolmentUserBatchRatingKarmaDF,ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)


def preComputeExternalEnrolment(spark: SparkSession,) -> DataFrame:
    externalEnrolmentDF= spark.read.parquet(ParquetFileConstants.EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE) \
        .withColumnRenamed("courseid", "content_id")
        
    exportDFToParquet(externalEnrolmentDF,ParquetFileConstants.EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE)
    

def preComputeUserOrgEnrolment(
    enrolmentDF: DataFrame,
    contentOrgDF: DataFrame,
    userOrgDF: DataFrame,
    spark: SparkSession,
) -> DataFrame:
    category_list = contentOrgDF.select("category") \
        .distinct() \
        .filter(F.col("category").isNotNull() & (F.col("category") != "")) \
        .rdd.map(lambda row: row["category"]).collect() 
    
    df = enrolmentDF.join(
        contentOrgDF, 
        on=["courseID"], 
        how="left"
    ).filter(
        F.col("category").isin(category_list)
    ).join(
        userOrgDF, 
        on=["userID"], 
        how="left"
    )
    
    completion_percentage_df = withCompletionPercentageColumn(df)
    old_completions_df = withOldCompletionStatusColumn(completion_percentage_df)
    final_df = withUserCourseCompletionStatusColumn(old_completions_df)
    
    return final_df


def withCompletionPercentageColumn(df: DataFrame) -> DataFrame:
    """Calculate completion percentage with boundary checks"""
    df = df.withColumn(
        "completionPercentage",
        F.expr("""
            CASE 
                WHEN courseResourceCount = 0 OR courseProgress = 0 OR dbCompletionStatus = 0 THEN 0.0 
                WHEN dbCompletionStatus = 2 THEN 100.0 
                ELSE 100.0 * courseProgress / courseResourceCount 
            END
        """)
    )
    # Apply boundary checks
    df = df.withColumn(
        "completionPercentage",
        F.expr("""
            CASE 
                WHEN completionPercentage > 100.0 THEN 100.0 
                WHEN completionPercentage < 0.0 THEN 0.0  
                ELSE completionPercentage 
            END
        """)
    )
    return df

def withOldCompletionStatusColumn(df: DataFrame) -> DataFrame:
    """
    completionPercentage   → completionStatus
    NULL                   → 'not-enrolled'
    0.0                    → 'enrolled'
    < 10.0                 → 'started'
    < 100.0                → 'in-progress'
    >= 100.0               → 'completed'
    """
    return df.withColumn(
        "completionStatus",
        F.expr("""
            CASE 
                WHEN completionPercentage IS NULL THEN 'not-enrolled'
                WHEN completionPercentage = 0.0 THEN 'enrolled'
                WHEN completionPercentage < 10.0 THEN 'started'
                WHEN completionPercentage < 100.0 THEN 'in-progress'
                ELSE 'completed'
            END
        """)
    )

def withUserCourseCompletionStatusColumn(df: DataFrame) -> DataFrame:
    """
    dbCompletionStatus     → userCourseCompletionStatus
    NULL                   → 'not-enrolled'
    0                      → 'not-started'
    1                      → 'in-progress'
    2 (else)               → 'completed'
    """
    return df.withColumn(
        "userCourseCompletionStatus",
        F.expr("""
            CASE 
                WHEN dbCompletionStatus IS NULL THEN 'not-enrolled'
                WHEN dbCompletionStatus = 0 THEN 'not-started'
                WHEN dbCompletionStatus = 1 THEN 'in-progress'
                ELSE 'completed'
            END
        """)
    )


def calculateCourseProgress(userCourseProgramCompletionDF):
    # Apply completion percentage column transformation
    df = withCompletionPercentageColumn(userCourseProgramCompletionDF)
    
    # Apply user course completion status column transformation
    df = withUserCourseCompletionStatusColumn(df)
    
    return df

def preComputeUserEnrolmentWarehouseData(spark):
    primary_categories = ["Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"]
    enrolmentDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
    userOrgDF = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
    contentOrgDF = spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE).filter(col("category").isin(primary_categories))
    allCourseProgramCompletionWithDetailsDFWithRating = preComputeUserOrgEnrolment(
        enrolmentDF, contentOrgDF, userOrgDF, spark
    )

    platform_enrolments_df = (
        allCourseProgramCompletionWithDetailsDFWithRating
            .withColumn("enrolledOn", date_format(col("courseEnrolledTimestamp"), ParquetFileConstants.DATE_TIME_FORMAT))
            .withColumn("firstCompletedOn", date_format(col("firstCompletedOn"), ParquetFileConstants.DATE_TIME_FORMAT))
            .select(
                col("userID"),
                col("courseID").alias("content_id"),
                col("firstCompletedOn").alias("first_completed_on"),
                col("userCourseCompletionStatus").alias("user_consumption_status"),
                col("certificateID"),
                col("enrolledOn").alias("enrolled_on"),
                col("batchID"),
                col("completionPercentage").alias("content_progress_percentage"),
                col("enrolment_status")
            )
            .dropDuplicates(["userID", "content_id", "batchID"])
        )
    
    externalEnrolmentDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE)
    externalContentOrgDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)
    
    marketplace_enrolments_df  = (
        externalContentOrgDF
            .join(externalEnrolmentDF, "content_id", "inner")
            .withColumn("courseEnrolledTimestamp", date_format(col("enrolled_date"), ParquetFileConstants.DATE_TIME_FORMAT))
            .withColumn("enrolment_status", lit("enrolled"))
            .withColumn("firstCompletedOn", 
                when(col("issued_certificates").isNull(), "")
                .otherwise(
                    when(size(col("issued_certificates")) > 0, 
                        date_format(col("issued_certificates")[0]["lastIssuedOn"], ParquetFileConstants.DATE_TIME_FORMAT))
                    .otherwise("")))
            .withColumn("certificateID", 
                when(col("issued_certificates").isNull(), "")
                .otherwise(col("issued_certificates")[size(col("issued_certificates")) - 1]["identifier"]))
            .withColumn("user_consumption_status",
                when(col("status").isNull(), "not-enrolled")
                .when(col("status") == 0, "not-started")
                .when(col("status") == 1, "in-progress")
                .otherwise("completed"))
            .select(
                col("userid").alias("userID"),
                col("content_id"),
                col("firstCompletedOn").alias("first_completed_on"),
                col("user_consumption_status"),
                col("certificateID"),
                col("courseEnrolledTimestamp").alias("enrolled_on"),
                lit("Not Available").alias("batchID"),
                col("completionPercentage").alias("content_progress_percentage"),
                col("enrolment_status")
            )
            .dropDuplicates(["userID", "content_id", "batchID"])
        )
    
    # create summary of igot and marketplace enrolments counts
    combined_enrolments_df = platform_enrolments_df.union(marketplace_enrolments_df)
    exportDFToParquet(combined_enrolments_df,ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
