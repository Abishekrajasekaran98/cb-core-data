from dfutil.user.userDFUtil import exportDFToParquet
from dfutil.content import contentDFUtil
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (col, lit, when, expr,coalesce, format_string, date_format, current_timestamp,
                                   to_date, from_json, explode_outer, size,min as F_min, max as F_max,
                                   count as F_count, sum as F_sum,broadcast, lower,avg,count)
from pyspark.sql.types import ( DateType)
from constants.ParquetFileConstants import ParquetFileConstants
from util import schemas
from dfutil.enrolment import enrolmentDFUtil

def esContentDataFrame(
    spark: SparkSession
) -> DataFrame:
    return spark.read.parquet(ParquetFileConstants.ESCONTENT_PARQUET_FILE)

def preComputeAllCourseProgramESDataFrame(spark: SparkSession) -> DataFrame:
  
    contentDF = esContentDataFrame(spark) \
        .withColumn("courseOrgID", explode_outer(col("createdFor"))) \
        .withColumn("contentLanguage", explode_outer(col("language"))) \
        .withColumn("contentCreator", explode_outer(col("organisation"))) \
        .withColumn("competencyAreaRefId", 
                   F.when(F.col("competencies_v6").isNotNull(), 
                         F.col("competencies_v6")["competencyAreaRefId"])
                   .otherwise(F.array())) \
        .withColumn("competencyThemeRefId", 
                   F.when(F.col("competencies_v6").isNotNull(), 
                         F.col("competencies_v6")["competencyThemeRefId"])
                   .otherwise(F.array())) \
        .withColumn("competencySubThemeRefId", 
                   F.when(F.col("competencies_v6").isNotNull(), 
                         F.col("competencies_v6")["competencySubThemeRefId"])
                   .otherwise(F.array())) \
        .select(
            col("identifier").alias("courseID"),
            col("primaryCategory").alias("category"),
            col("name").alias("courseName"),
            col("status").alias("courseStatus"),
            col("reviewStatus").alias("courseReviewStatus"),
            col("channel").alias("courseChannel"),
            col("lastPublishedOn").alias("courseLastPublishedOn"),
            col("duration").cast(FloatType()).alias("courseDuration"),
            col("leafNodesCount").alias("courseResourceCount"),
            col("lastStatusChangedOn").alias("lastStatusChangedOn"),
            col("programDirectorName"),
            col("courseOrgID"),
            col("badgeDetails_v1"),
            F.col("competencyAreaRefId"),
            F.col("competencyThemeRefId"),
            F.col("competencySubThemeRefId"),
            col("contentLanguage"),
            col("courseCategory"),
            col("contentCreator"),
            col("difficultyLevel"),
            col("endDate")
        ) \
        .dropDuplicates(["courseID", "category"]) \
        .fillna(0.0, subset=["courseDuration"]) \
        .fillna(0, subset=["courseResourceCount"])
        
    exportDFToParquet(contentDF,ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE)

def preComputeRatingAndSummaryDataFrame(spark):
    ratingSummaryDF = spark.read.parquet(ParquetFileConstants.RATING_SUMMARY_PARQUET_FILE) \
        .where(expr("total_number_of_ratings > 0")) \
        .withColumn("ratingAverage", expr("sum_of_total_ratings / total_number_of_ratings")) \
        .select(
            col("activityid").alias("courseID"),
            col("activitytype").alias("categoryLower"),
            col("sum_of_total_ratings").alias("ratingSum"),
            col("total_number_of_ratings").alias("ratingCount"),
            col("ratingAverage"),
            col("totalcount1stars").alias("count1Star"),
            col("totalcount2stars").alias("count2Star"),
            col("totalcount3stars").alias("count3Star"),
            col("totalcount4stars").alias("count4Star"),
            col("totalcount5stars").alias("count5Star")
        ) \
        .withColumn("categoryLower", lower(col("categoryLower"))) \
        .dropDuplicates(["courseID", "categoryLower"])
    
    exportDFToParquet(ratingSummaryDF,ParquetFileConstants.RATING_SUMMARY_COMPUTED_PARQUET_FILE)

    ratingRawDF = spark.read.parquet(ParquetFileConstants.RATING_PARQUET_FILE)
    ratingDF = ratingRawDF \
        .select(
            col("activityid").alias("courseID"),
            col("userid").alias("userID"), 
            col("rating").alias("userRating"),
            col("activitytype").alias("cbpType"),
            col("createdon").alias("createdOn")
        )
    exportDFToParquet(ratingDF,ParquetFileConstants.RATING_COMPUTED_PARQUET_FILE)


    contentRating = ratingDF.filter(col("userRating").isNotNull()) \
    .groupBy("courseID") \
    .agg(
        count("userRating").alias("totalRatings"),
        avg("userRating").alias("rating")
    ) \
    .select("courseID", "totalRatings", "rating")

    exportDFToParquet(contentRating,ParquetFileConstants.CONTENT_RATING_COMPUTED_PARQUET_FILE)

def preComputeContentDataFrame(spark: SparkSession):
    contentRatingDF=spark.read.parquet(ParquetFileConstants.CONTENT_RATING_COMPUTED_PARQUET_FILE)    
    contentWithRatingDF=spark.read.parquet(ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE).join(
        contentRatingDF,"courseID","left").drop(contentRatingDF["courseID"])
    
    orgDF=spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE).select(
      col("orgID").alias("courseOrgID"),
      col("orgName").alias("courseOrgName"),
      col("orgStatus").alias("courseOrgStatus")
    )

    contentWithOrgRatingDF=contentWithRatingDF.join(orgDF, contentWithRatingDF["courseOrgID"] == orgDF["courseOrgID"], "left") \
        .drop(orgDF["courseOrgID"]) \
        
    exportDFToParquet(contentWithOrgRatingDF,ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)


def preComputeExternalContentDataFrame(spark) -> DataFrame:
        df = (
            spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_PARQUET_FILE)
            .withColumn("parsed_data", from_json(col("cios_data"), schemas.cios_data_schema))
            .withColumnRenamed("courseid", "content_id")
            .select(
                col("content_id"),
                col("parsed_data.content.name").alias("courseName"),
                col("parsed_data.content.duration").alias("courseDuration"),
                col("parsed_data.content.lastUpdatedOn").alias("courseLastPublishedOn"),
                col("parsed_data.content.contentPartner.id").alias("courseOrgID"),
                col("parsed_data.content.contentPartner.contentPartnerName").alias("courseOrgName"),
                col("parsed_data.content.badgeDetails_v1").alias("badge"),
                lit("External Content").alias("category"),
                lit("LIVE").alias("courseStatus")
            )
        )
        exportDFToParquet(df,ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)

#=======================latest hierarchy code ==========================
LEVEL_NAMES = {
    1: "root",
    2: "first_level_child",
    3: "second_level_child",
    4: "third_level_child",
    5: "fourth_level_child",
    6: "fifth_level_child",
}
HIERARCHY_COLS = ["root_content_id", "level", "parent_id", "node_type"]

def get_ancestor_descendant_pairs(df: DataFrame, id_col: str,
                                  list_cols=("childNodes", "leafNodes")) -> DataFrame:
    """
    One row per (ancestor_id, descendant_id).
    Programs list everything in childNodes; course units may list their
    resources only in leafNodes, so both columns are needed.
    """
    available = [c for c in list_cols if c in df.columns]
    if not available:
        return df.sparkSession.createDataFrame([], "ancestor_id string, descendant_id string")

    lists_without_nulls = [F.coalesce(F.col(c), F.array().cast("array<string>")) for c in available]
    all_listed_ids = F.array_distinct(F.concat(*lists_without_nulls))

    return (df.select(F.col(id_col).alias("ancestor_id"),
                      F.explode(all_listed_ids).alias("descendant_id"))
            .withColumn("descendant_id", F.trim("descendant_id"))
            .filter(F.col("descendant_id").isNotNull()
                    & (F.col("descendant_id") != "")
                    & (F.col("descendant_id") != F.col("ancestor_id")))
            .distinct())

def get_leaf_ids(df: DataFrame, leaf_col: str = "leafNodes") -> DataFrame:
    """Every id that appears in any leafNodes list = a lowermost child."""
    if leaf_col not in df.columns:
        return df.sparkSession.createDataFrame([], "leaf_id string")
    return (df.select(F.explode(F.col(leaf_col)).alias("leaf_id"))
            .withColumn("leaf_id", F.trim("leaf_id"))
            .filter(F.col("leaf_id").isNotNull() & (F.col("leaf_id") != ""))
            .distinct())

def get_direct_parent_edges(pairs: DataFrame) -> DataFrame:
    """
    Keeps only the immediate parent of each node.
    An ancestor is NOT the direct parent if another ancestor of the same
    node sits under it (Program -> Course -> Module -> Resource:
    for the Resource, Program and Course are dropped, Module is kept).
    """
    candidate = pairs.select(F.col("ancestor_id").alias("candidate_parent_id"),
                             "descendant_id")
    other = pairs.select(F.col("ancestor_id").alias("other_ancestor_id"),
                         "descendant_id")
    under_candidate = pairs.select(F.col("ancestor_id").alias("candidate_parent_id"),
                                   F.col("descendant_id").alias("other_ancestor_id"))

    not_direct_parents = (candidate.join(other, "descendant_id")
                          .filter(F.col("candidate_parent_id") != F.col("other_ancestor_id"))
                          .join(under_candidate, ["candidate_parent_id", "other_ancestor_id"])
                          .select(F.col("candidate_parent_id").alias("ancestor_id"), "descendant_id")
                          .distinct())

    return (pairs.join(not_direct_parents, ["ancestor_id", "descendant_id"], "left_anti")
            .select(F.col("ancestor_id").alias("parent_id"),
                    F.col("descendant_id").alias("child_id")))

def get_root_ids(df: DataFrame, all_nodes: DataFrame, edges: DataFrame,
                 id_col: str, category_col: str = "courseCategory") -> DataFrame:
    """
    A node is a root if either:
      1. it is never anyone's child (top of a tree), or
      2. it has a non-empty courseCategory - i.e. it can be consumed on its
         own, even when it also sits inside a program. Such a node gets its
         own tree in addition to appearing as a child under each parent.

    Course Units, resources and question sets have no courseCategory
    (NULL or ""), so they never become roots through rule 2.
    """
    top_roots = (all_nodes
                 .join(edges, all_nodes.node_id == edges.child_id, "left_anti")
                 .select("node_id"))

    if category_col not in df.columns:
        return top_roots

    standalone_roots = (df
                        .filter(F.length(F.trim(F.col(category_col))) > 0)
                        .select(F.col(id_col).alias("node_id")))

    return top_roots.unionByName(standalone_roots).distinct()

def build_content_hierarchy_from_es(spark: SparkSession):
    df = spark.read.parquet(ParquetFileConstants.ESCONTENT_EXTENDED_PARQUET_FILE)
    id_col="identifier"
    max_depth=5

    pairs = get_ancestor_descendant_pairs(df, id_col).cache()
    edges = get_direct_parent_edges(pairs).cache()

    all_nodes = (df.select(F.col(id_col).alias("node_id"))
                 .unionByName(edges.select(F.col("child_id").alias("node_id")))
                 .distinct())

    # roots = never anyone's child
    roots = get_root_ids(df, all_nodes, edges, id_col)

    current_level = roots.select(
        "node_id",
        F.col("node_id").alias("root_content_id"),
        F.lit(None).cast("string").alias("parent_id"),
        F.lit(1).alias("level_num"),
        F.array("node_id").alias("path"),
    ).localCheckpoint()
    result = current_level

    # walk down one level at a time
    for _ in range(max_depth - 1):
        next_level = (current_level.alias("cur")
        .join(edges.alias("e"), F.col("cur.node_id") == F.col("e.parent_id"))
        .filter(~F.array_contains(F.col("cur.path"), F.col("e.child_id")))  # cycle guard
        .select(
            F.col("e.child_id").alias("node_id"),
            F.col("cur.root_content_id"),
            F.col("cur.node_id").alias("parent_id"),
            (F.col("cur.level_num") + 1).alias("level_num"),
            F.concat(F.col("cur.path"), F.array(F.col("e.child_id"))).alias("path"),
        )).localCheckpoint()
        if next_level.limit(1).count() == 0:
            break
        result = result.unionByName(next_level)
        current_level = next_level

    # level name: first_level_child, second_level_child, ...
    level_map = F.create_map(*[F.lit(v) for kv in LEVEL_NAMES.items() for v in kv])
    result = result.withColumn(
        "level",
        F.coalesce(level_map[F.col("level_num")],
                   F.concat(F.lit("level_"), (F.col("level_num") - 1).cast("string"),
                            F.lit("_child"))))

    # nodes not reachable from any root (cycles): keep them with whatever is known
    orphans = all_nodes.join(result.select("node_id"), "node_id", "left_anti")

    orphan_rows = (orphans.join(edges, orphans.node_id == edges.child_id, "left")
                   .select("node_id",
                           F.lit(None).cast("string").alias("root_content_id"),
                           F.lit("unmapped").alias("level"),
                           "parent_id"))

    result = (result.select("node_id", "root_content_id", "level", "parent_id")
              .unionByName(orphan_rows))

    other_cols = [c for c in df.columns if c != id_col]
    out = (result.join(df, result.node_id == df[id_col], "left")
           .withColumn(id_col, F.coalesce(F.col(id_col), F.col("node_id")))
           .select(id_col, "root_content_id", "level", "parent_id", *other_cols))

    exportDFToParquet(out, ParquetFileConstants.CONTENT_HIERARCHY_FLATTENED_ENHANCED_PARQUET_FILE)
    pairs.unpersist()
    edges.unpersist()

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

def allCourseProgramDetailsWithCompetenciesJsonDataFrame(
    allCourseProgramESDF: DataFrame, 
    hierarchyDF: DataFrame, 
    orgDF: DataFrame, 
) -> DataFrame:
    # Add hierarchy column with competencies
    df = addHierarchyColumn(
        allCourseProgramESDF, 
        hierarchyDF, 
        "courseID", 
        "data", 
        competencies=True
    ).withColumn("competenciesJson", col("data.competencies_v3"))
    
    # Add course organization details and clean up
    courseOrgDetailsDF = addCourseOrgDetails(df, orgDF) \
        .fillna(0.0, subset=["courseDuration"]) \
        .fillna(0, subset=["courseResourceCount"]) \
        .drop("data")
    
    return courseOrgDetailsDF

def addHierarchyColumn(
    df: DataFrame, 
    hierarchyDF: DataFrame, 
    idCol: str, 
    asCol: str,
    children: bool = False, 
    competencies: bool = False, 
    l2Children: bool = False,
) -> DataFrame:
    # Get the hierarchy schema based on the flags
    hierarchySchema = schemas.make_hierarchy_schema(children, competencies, l2Children)
    
    result_df = df.join(
        hierarchyDF, 
        df[idCol] == hierarchyDF["identifier"], 
        "left"
    ) \
    .fillna("{}", subset=["hierarchy"]) \
    .withColumn(asCol, from_json(col("hierarchy"), hierarchySchema)) \
    .drop("hierarchy")
    
    return result_df

def addCourseOrgDetails(
    courseDF: DataFrame, 
    orgDF: DataFrame
) -> DataFrame:
    # Prepare organization DataFrame with renamed columns
    joinOrgDF = orgDF.select(
        col("orgID").alias("courseOrgID"),
        col("orgName").alias("courseOrgName"),
        col("orgStatus").alias("courseOrgStatus")
    )
    df = courseDF.join(joinOrgDF, ["courseOrgID"], "left")
    
    return df

def preComputeContentWarehouseData(spark):
    """
    Generate ONLY content warehouse data 
    Removes dependency on full course report job
    """
    try:
        currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)
        primary_categories = ["Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"]
        
        print("Starting Content Warehouse Data Generation...")
        
        # Load required data
        print("Loading platform data...")
        allCourseProgramDetailsDF = spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE).filter(col("category").isin(primary_categories))
        contentHierarchyDF = (spark.read.parquet(ParquetFileConstants.CONTENT_HIERARCHY_FLATTENED_ENHANCED_PARQUET_FILE)
                              .select("identifier", "root_content_id", "parent_id", "mimeType"))
        enrolmentDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
        courseBatchDF = spark.read.parquet(ParquetFileConstants.BATCH_SELECT_PARQUET_FILE)
        
        print("Loading marketplace data...")
        marketPlaceEnrolmentsDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE)
        marketPlaceContentsDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)
        
        # Process Platform Content Data
        print("Processing platform course progress and enrollments...")
        courseResCountDF = allCourseProgramDetailsDF.select("courseID", "courseResourceCount")
        userEnrolmentDF = enrolmentDF.join(courseResCountDF, on=["courseID"], how="left")
        allCBPCompletionWithDetailsDF = enrolmentDFUtil.calculateCourseProgress(userEnrolmentDF)

        # Aggregate course completion details
        aggregatedDF = allCBPCompletionWithDetailsDF.groupBy("courseID") \
            .agg(
                F_min("courseCompletedTimestamp").alias("earliestCourseCompleted"),
                F_max("courseCompletedTimestamp").alias("latestCourseCompleted"),
                F_count(lit(1)).alias("enrolledUserCount"),
                F_sum(when(col("userCourseCompletionStatus") == "in-progress", 1).otherwise(0)).alias("inProgressCount"),
                F_sum(when(col("userCourseCompletionStatus") == "not-started", 1).otherwise(0)).alias("notStartedCount"),
                F_sum(when(col("userCourseCompletionStatus") == "completed", 1).otherwise(0)).alias("completedCount"),
                F_sum("issuedCertificateCountPerContent").alias("totalCertificatesIssued")
            ) \
            .withColumn("firstCompletedOn", to_date(col("earliestCourseCompleted"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("lastCompletedOn", to_date(col("latestCourseCompleted"), ParquetFileConstants.DATE_FORMAT))

        allCBPAndAggDF = allCourseProgramDetailsDF.join(aggregatedDF, ["courseID"], "left")

        # Add batch information
        curatedCourseDataDFWithBatchInfo = allCBPAndAggDF \
            .join(
                broadcast(
                    allCourseProgramDetailsDF
                    .filter(col("category") == "Blended Program")
                    .select("courseID")
                    .join(courseBatchDF, ["courseID"], "inner")
                    .select("courseID", "batchID", "courseBatchName", "courseBatchStartDate", "courseBatchEndDate")
                ), 
                ["courseID"], 
                "left"
            )

        # Format and filter platform data
        fullDF = contentDFUtil.duration_format(curatedCourseDataDFWithBatchInfo, "courseDuration") \
            .filter(col("courseStatus").isin(["Live", "Draft", "Retired", "Review"])) \
            .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("courseBatchStartDate", to_date(col("courseBatchStartDate"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("lastStatusChangedOn", to_date(col("lastStatusChangedOn"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("ArchivedOn", when(col("courseStatus") == "Retired", to_date(col("lastStatusChangedOn"), ParquetFileConstants.DATE_FORMAT)))

        # Process Marketplace Content Data
        print("Processing marketplace course data...")
        marketPlaceEnrolmentsAggDF = marketPlaceEnrolmentsDF \
            .withColumn("issuedCertificateCountPerContent", 
                        when(size(col("issued_certificates")) > 0, lit(1)).otherwise(lit(0))) \
            .groupBy("content_id") \
            .agg(
                F_count(lit(1)).alias("enrolledUserCount"),
                F_sum(when(col("status") == 1, 1).otherwise(0)).alias("inProgressCount"),
                F_sum(when(col("status") == 0, 1).otherwise(0)).alias("notStartedCount"),
                F_sum(when(col("status") == 2, 1).otherwise(0)).alias("completedCount"),
                F_sum(col("issuedCertificateCountPerContent")).alias("totalCertificatesIssued"),
                F_min("completedon").alias("earliestCompletedOn"),
                F_max("completedon").alias("latestCompletedOn")
            )

        marketPlaceContentWithEnrolmentsDF = contentDFUtil.duration_format(marketPlaceContentsDF, "courseDuration") \
            .join(marketPlaceEnrolmentsAggDF, ["content_id"], "outer") \
            .withColumn("firstCompletedOn", to_date(col("earliestCompletedOn"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("lastCompletedOn", to_date(col("latestCompletedOn"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("data_last_generated_on", currentDateTime)

        # Generate SCORM Detection for Platform Content
        print("Detecting SCORM content...")
        childNodesDF = contentHierarchyDF.filter(col("identifier") != col("root_content_id"))

        # Level 1: direct children of the course
        level1NodesDF = childNodesDF.filter(col("parent_id") == col("root_content_id"))

        # Level 2: children of level-1 nodes
        level2NodesDF = childNodesDF.join(
            level1NodesDF.select("root_content_id", col("identifier").alias("parent_id")),
            ["root_content_id", "parent_id"],
            "inner"
        )

        scorm_detection_df = level1NodesDF.select("root_content_id", "mimeType") \
            .unionByName(level2NodesDF.select("root_content_id", "mimeType")) \
            .filter(col("mimeType").endswith("html-archive")) \
            .select(col("root_content_id").alias("courseID")) \
            .distinct() \
            .withColumn("scorm_flag", lit(1))

        # Generate Platform Content Warehouse Data
        print("Generating platform content warehouse data...")
        platformContentWarehouseDF = fullDF \
            .join(scorm_detection_df, ["courseID"], "left") \
            .na.fill(0, ["scorm_flag"]) \
            .withColumn("data_last_generated_on", currentDateTime) \
            .select(
                col("courseID").alias("content_id"),
                col("courseOrgID").alias("content_provider_id"),
                when(
                    col("courseOrgName").isNotNull() & 
                    (col("courseOrgName") != "") &
                    (col("courseOrgName") != " "),
                    col("courseOrgName")
                ).otherwise(col("contentCreator")).alias("content_provider_name"),
                col("courseName").alias("content_name"),
                col("category").alias("content_type"),
                col("batchID").alias("batch_id"),
                col("courseBatchName").alias("batch_name"),
                col("courseBatchStartDate").alias("batch_start_date"),
                col("courseBatchEndDate").alias("batch_end_date"),
                col("courseDuration").alias("content_duration"),
                col("rating").alias("content_rating"),
                date_format(col("courseLastPublishedOn"), ParquetFileConstants.DATE_FORMAT).alias("last_published_on"),
                col("ArchivedOn").alias("content_retired_on"),
                col("courseStatus").alias("content_status"),
                col("courseResourceCount").alias("resource_count"),
                col("totalCertificatesIssued").alias("total_certificates_issued"),
                col("courseReviewStatus").alias("content_substatus"),
                col("contentLanguage").alias("language"),
                col("courseCategory").alias("content_sub_type"),
                col("scorm_flag"),
                col("data_last_generated_on")
            )

        # Generate Marketplace Content Warehouse Data
        print("Generating marketplace content warehouse data...")
        marketPlaceContentWarehouseDF = marketPlaceContentWithEnrolmentsDF.select(
            col("content_id"),
            col("courseOrgID").alias("content_provider_id"),
            col("courseOrgName").alias("content_provider_name"),
            col("courseName").alias("content_name"),
            col("category").alias("content_type"),
            lit("Not Available").alias("batch_id"),
            lit("Not Available").alias("batch_name"),
            lit(None).cast(DateType()).alias("batch_start_date"),
            lit(None).cast(DateType()).alias("batch_end_date"),
            col("courseDuration").alias("content_duration"),
            lit("Not Available").alias("content_rating"),
            to_date(col("courseLastPublishedOn"), ParquetFileConstants.DATE_FORMAT).alias("last_published_on"),
            lit(None).cast(DateType()).alias("content_retired_on"),
            col("courseStatus").alias("content_status"),
            lit("Not Available").alias("resource_count"),
            col("totalCertificatesIssued").alias("total_certificates_issued"),
            lit("Not Available").alias("content_substatus"),
            lit("Not Available").alias("language"),
            lit("External Content").alias("content_sub_type"),
            lit("0").alias("scorm_flag"),
            col("data_last_generated_on")
        )

        # Combine Platform and Marketplace Data & Write to Warehouse
        print("Writing content warehouse data...")
        df_warehouse = platformContentWarehouseDF.union(marketPlaceContentWarehouseDF)
        exportDFToParquet(df_warehouse, ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE)

    except Exception as e:
        print(f"Content warehouse data generation error: {str(e)}")
        raise

def writeWarehouseParquetFiles(spark, config):
    """
    Write parquet files for org hierarchy, events, and event enrollments
    This can be decoupled and run as a separate job if needed
    """
    try:
        print("📦 Writing parquet files to warehouse...")
        
        output_path = getattr(config, 'baseCachePath', '/home/analytics/pyspark/data-res/pq_files/cache_pq/')
        warehouse_path = config.warehouseReportDir
        
        # Read and write org hierarchy
        org_hierarchy = spark.read.parquet(f"{output_path}/orgHierarchy") \
            .withColumn("mdo_created_on", to_date(col("mdo_created_on")).cast("string"))
        
        org_hierarchy.write.mode("overwrite").option("compression", "snappy").parquet(
            f"{warehouse_path}/{config.dwOrgTable}")
        print(f"✓ Written: {warehouse_path}/{config.dwOrgTable}")
        
        # Read and write events
        events = spark.read.parquet(f"{output_path}/eventDetails") \
            .select("event_id", "event_name", "event_provider_mdo_id", "event_start_datetime",
                    F.col("duration").cast("string").alias("duration"), "event_status", "event_type", 
                    "presenters", "video_link", "recording_link","event_tag","speaker_id", "speaker_name",F.col("typeofEvent").alias("type_of_event"),
                    F.col("maxEnrolments").alias("max_enrolments"), F.col("meetingAgenda").alias("meeting_agenda"),
                    F.col("recordedMediaLink").alias("recorded_media_link"), 
                    F.col("noOfAttendes").alias("no_of_attendes"),F.col("eventDuration").alias("event_duration"), 
                    F.col("meetingSummary").alias("meeting_summary"), F.col("courseLinked").alias("course_linked")
    )
 
        
        events.write.mode("overwrite").option("compression", "snappy").parquet(
            f"{warehouse_path}/event_details")
        print(f"✓ Written: {warehouse_path}/event_details")
        
        # Read and process event enrollments with karma points
        eventEnrolmentsDF = spark.read.parquet(f"{output_path}/eventEnrolmentDetails")
        
        karmaPointsData = spark.read.parquet(f"{output_path}/userKarmaPoints") \
            .select(F.col("userid").alias("user_id"),
                    F.col("context_id").alias("event_id"),
                    F.col("points")) \
            .withColumn("points",
                        F.when(F.col("points").cast("int").isNotNull(), F.col("points").cast("int")).otherwise(F.lit(0))) \
            .groupBy("user_id", "event_id") \
            .agg(F.sum("points").alias("karma_points"))
        
        eventsEnrolmentDataDFWithKarmaPoints = eventEnrolmentsDF.join(karmaPointsData, ["user_id", "event_id"], "left")
        
        eventsEnrolmentDataDFWithKarmaPoints.write.mode("overwrite").option("compression", "snappy").parquet(
            f"{warehouse_path}/event_enrolment_details")
        print(f"✓ Written: {warehouse_path}/event_enrolment_details")
        
        print("✅ All parquet files written successfully!")
        
    except Exception as e:
        print(f"❌ Error writing parquet files: {str(e)}")
        raise