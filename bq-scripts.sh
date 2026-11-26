# Set your project ID
PROJECT_ID="prj-kb-prd-looker-gcp-1014"

# Set the dataset name
DATASET_NAME="kb_prod_dataset"

# cloud storage folder name
STORAGE_FOLDER="kb_prod_avro"

# Loop over each table name
for TABLE_NAME in "assessment_detail" "bp_enrolments" "cb_plan" "content" "content_resource" "kcm_content_mapping" "kcm_dictionary" "org_hierarchy" "user_detail" "user_enrolments" "event_details" "event_enrolment_details" "org_hierarchy_new" "karma_coin_transaction"; do
#for TABLE_NAME in "baseline_assessment_submission" "endline_assessment_submission"; do
  echo "Processing table: $TABLE_NAME"

  # Delete all existing avro files in Cloud Storage
  gsutil rm -a gs://$STORAGE_FOLDER/$TABLE_NAME/*.parquet

  # Copy avro files to Cloud Storage
  gsutil cp /home/analytics/pyspark/warehouse/$TABLE_NAME/part*.parquet gs://$STORAGE_FOLDER/$TABLE_NAME/

  # delete existing table
  bq rm -f $DATASET_NAME.$TABLE_NAME

  # create empty
  bq mk --table $DATASET_NAME.$TABLE_NAME

  # Load data into the BigQuery table
  bq load \
  --source_format=PARQUET \
  $DATASET_NAME.$TABLE_NAME \
  "gs://$STORAGE_FOLDER/$TABLE_NAME/*.parquet"

  # Delete all avro files in Cloud Storage post BQ push
  gsutil rm -a gs://$STORAGE_FOLDER/$TABLE_NAME/*.parquet

  echo "Completed processing for table: $TABLE_NAME"

done