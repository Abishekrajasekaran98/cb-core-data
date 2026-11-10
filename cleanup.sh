#!/bin/bash
# ===== 1. LOGGING SETUP =====
log_file="/home/analytics/pyspark/cleanup.log"
exec &>> "$log_file"
echo "Running script at $(date)"
# 🎯 PURPOSE: All output (stdout & stderr) goes to log file for monitoring

# ===== 2. REPORT TYPES DEFINITION =====
data_reports=(
    'course-report'
    'user-report'
    'user-enrollment-report'
    'cba-report'
    'org-hierarchy-report'
    'blended-program-report-cbp'
    'cbp-report-mdo-enrolment'
    'user-assessment-report-cbp'
    'blended-program-report-mdo'
    'cbp-report'
    'cbp-report-mdo-summary'          # FIXED: had a trailing space inside the quotes, so it never matched the real folder
    'kcm-report'
    'course-completion-survey-report' # ADDED
    'gamification-report'             # ADDED
    'ml-report'                       # ADDED
)
# 🎯 PURPOSE: Report folders under standalone-reports whose old dated folders get cleaned

# ===== 3. DIRECTORY SETUP =====
source_dir="/home/analytics/pyspark/reports/standalone-reports"     # Where reports are generated
# destination_dir="/home/analytics/pyspark/datadownload/data"              # Where reports are copied for download
# 🎯 PURPOSE: Source = production reports, Destination = download staging area

# ===== 4. CLEANUP OLD DATA FROM SOURCE =====
echo "Removing $source_dir old data"
cd $source_dir

# Calculate today's and yesterday's date
# (today was previously assigned further down, so the log line below printed it empty)
today=$(date "+%Y-%m-%d")
yesterday=$(date -d "yesterday" "+%Y-%m-%d")

# ADDED: if the date ever comes back empty, the paths below would collapse to the
# report folder itself and rm -rf would wipe the whole report type. Abort instead.
if [ -z "$yesterday" ]; then
    echo "ERROR: could not compute yesterday's date - aborting cleanup"
    exit 1
fi

echo "Today: $today, Yesterday: $yesterday"

for dir in "${data_reports[@]}"; do
    yesterday_folder="$source_dir/$dir/$yesterday"
    if [ -d "$yesterday_folder" ]; then
        echo "Removing: $yesterday_folder"
        rm -rf "$yesterday_folder"
    else
        echo "Not found: $yesterday_folder"
    fi
done
# 🎯 PURPOSE: Clean up old report files from production directory (keep only 1 day)

# ===== 5. DATE CALCULATIONS =====
today=$(date "+%Y-%m-%d")                               # 2024-08-21
yesterday=$(date -d "yesterday" "+%Y-%m-%d")             # 2024-08-20  
day_before_yesterday=$(date -d "$yesterday - 1 day" "+%d%b")  # 19Aug
# 🎯 PURPOSE: Calculate dates for file naming and cleanup operations

# ===== 6. PREPARE DESTINATION DIRECTORIES =====
# echo "Removing the directories under - $destination_dir"
# rm -rf "$destination_dir"                               # Delete entire download staging area

# echo "Creating the subdirectories directories - $destination_dir"
# for dir in "${data_reports[@]}"; do
#     mkdir -p "$destination_dir/$dir/$today-warehouse"   # Create fresh directories for today's reports
# done
# 🎯 PURPOSE: Clean slate for today's report downloads

# ===== 7. AUDIT SOURCE DIRECTORIES =====
# for dir in "${data_reports[@]}"; do
#     echo "Running ls -latr of $source_dir/$dir"
#     ls -latr "$source_dir/$dir/"*                       # List contents for debugging
# done
# 🎯 PURPOSE: Log what's available in source before copying

# # ===== 8. COPY TODAY'S REPORTS =====
# echo "Copying the full reports to $destination_dir"
# for dir in "${data_reports[@]}"; do
#     cp -r "$source_dir/$dir/"$today*warehouse "$destination_dir/$dir"  # Copy today's warehouse reports
# done
# chmod -R 777 "$destination_dir"                         # Make everything accessible
# # 🎯 PURPOSE: Stage today's reports for download

# ===== 9. CREATE BACKUP ZIP =====
# echo "Creating the zip"
# cd /home/analytics/pyspark/datadownload
# zip -r "/home/analytics/pyspark/report_backup/$(date "+%d%b").zip" data/     # Create zip named like "21Aug.zip"
# chmod 777 "/home/analytics/pyspark/report_backup/$(date "+%d%b").zip"
# rm "/home/analytics/pyspark/report_backup/$day_before_yesterday.zip"          # Remove 2-day-old backup
# # 🎯 PURPOSE: Create compressed backup and maintain 2-day retention

# ===== 10. CLEANUP YESTERDAY'S REPORTS FROM SOURCE =====
echo "Removing yesterdays's report from $source_dir"
# for dir in "${data_reports[@]}"; do
    # rm -rf "$source_dir/$dir/$yesterday-full"
    # rm -rf "$source_dir/$dir/$yesterday-warehouse" 
    rm -rf "$source_dir/merged"
    rm -rf "/home/analytics/pyspark/warehouse/fullReport"
# done
# 🎯 PURPOSE: Remove yesterday's reports from production directory

# # ===== 11. LOG CLEANUP =====
# echo "Removing the logs from /home/analytics/pyspark/logs/data-products and keeping the logs for 3 days"
# cd /home/analytics/pyspark/logs/data-products/
# find . -atime +2 -exec rm {} \;                         # Remove log files not accessed for 2+ days
# # 🎯 PURPOSE: Prevent log directory from filling up disk space

# ===== 12. CLEANUP WAREHOUSE FOLDERS =====
# Deletes everything in the warehouse folder EXCEPT the .py scripts and content_health_score.
# Names only - each entry must be a direct child of $warehouse_dir (no slashes).
warehouse_dir="/home/analytics/pyspark/warehouse"
warehouse_delete=(
    'L_org_hierarchy'
    'aiCBPFactTable'
    'apar_cbp_enrollment'
    'assessment_detail'
    'bp_enrolments'
    'cap_allocation_meta'
    'cap_allocation_user_wise'
    'cb_plan'
    'cbp_enrollments'
    'competencyMasterListTable'
    'content'
    'content_custom'
    'content_resource'
    'course_completion_survey_details'
    'event_details'
    'event_enrolment_details'
    'funnelStageFactsTable'
    'karma_dispatch'
    'kcm'
    'kcm_content_mapping'
    'kcm_dictionary'
    'learnerCoverageMapTable'
    'mdo_enrolment_fact'
    'mdo_org_access'
    'mdo_user_access'
    'mdo_user_summary'
    'myigot_building'
    'national_priority_mapping'
    'nlw_mdo_leaderboard'
    'orgDeptUserSummaryTable'
    'org_hierarchy'
    'org_hierarchy_new'
    'recommendedCourseCompetencyMapTable'
    'savedCourseMapTable'
    'temp'
    'unified'
    'unified_logs'
    'userCustomFields'
    'user_custom_report'
    'user_detail'
    'user_enrolments'
    'user_enrolments-1'
    'user_enrolments_custom'
)

# Never delete these, even if someone adds them to the list above by mistake.
# (.py scripts are also protected by the *.py check inside the loop.)
warehouse_protected=(
    'content_health_score'
)

echo "Cleaning warehouse folders under $warehouse_dir"
for name in "${warehouse_delete[@]}"; do
    if [ -z "$name" ] || [[ "$name" == */* ]]; then
        echo "Skipping invalid entry: '$name'"
        continue
    fi
    if [[ "$name" == *.py ]]; then
        echo "Skipping protected script: $name"
        continue
    fi
    skip=0
    for p in "${warehouse_protected[@]}"; do
        if [ "$name" == "$p" ]; then skip=1; fi
    done
    if [ "$skip" -eq 1 ]; then
        echo "Skipping protected entry: $name"
        continue
    fi
    target="$warehouse_dir/$name"
    if [ -e "$target" ]; then
        echo "Removing: $target"
        rm -rf "$target"
    else
        echo "Not found: $target"
    fi
done
# 🎯 PURPOSE: Remove warehouse folders that are no longer needed
