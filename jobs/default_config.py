import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

DEFAULT_CONFIG = {
    # Debug and Validation
    'debug': 'true',
    'validation': 'true',

    # Redis Configuration
    'redisHost': '10.175.5.20',
    'redisKpHost':'10.175.5.19',
    'redisPort': '6379',
    'redisDB': '12',

    # Kfaka configuration
    'brokerList': '10.175.4.154:9092,10.175.4.155:9092,10.175.4.208:9092',
    'compression': None,

    # Spark Connection Hosts
    'sparkCassandraConnectionHost': '192.168.3.211',
    'sparkDruidRouterHost': '192.168.3.91',
    'sparkElasticsearchConnectionHost': '192.168.3.211',
    'sparkElasticsearchAuditConnectionHost': '10.175.4.10',
    'sparkElasticsearchConnectionPort': '9200',
    'sparkMongoConnectionHost': '192.168.3.178',
    'sparkIGotElasticsearchConnectionHost':'10.175.3.36',

    # External Service Configuration
    'fracBackendHost': 'frac-dictionary.karmayogi.nic.in',

    # ML/Survey Configuration
    'solutionIDs': '',
    'mlMongoDatabase': 'ml-survey',
    'mlSparkDruidRouterHost': '192.168.3.91',
    'mlSparkMongoConnectionHost': '192.168.3.178',
    'surveyCollection': 'solutions',
    'reportConfigCollection': 'dataProductConfigurations',
    'gracePeriod': '2',
    'baseUrlForEvidences': 'www.https://igotkarmayogi.gov.in/',
    'mlReportPath': 'standalone-reports/ml-report',
    'includeExpiredSolutionIDs': 'true',

    # Batch Size Configuration
    'SurveyQuestionReportBatchSize': '2000',
    'SurveyStatusReportBatchSize': '20000',
    'ObservationQuestionReportBatchSize': '2000',
    'ObservationStatusReportBatchSize': '15000',

    # PostgreSQL Application Database
    'appPostgresHost': '192.168.3.178:5432',
    'appPostgresUsername': 'sunbird',
    'appPostgresCredential': 'sunbird',
    'appPostgresSchema': 'sunbird',
    'appOrgHierarchyTable': 'org_hierarchy_v4',
    'postgresCompetencyTable': 'data_node',
    'postgresCompetencyHierarchyTable': 'node_mapping',

    # PostgreSQL Data Warehouse
    'dwPostgresHost': '192.168.3.211:5432',
    'dwPostgresUsername': 'postgres',
    'dwPostgresCredential': 'Password@12345678',
    'dwPostgresSchema': 'warehouse',
    'dwUserActivityTable': 'user_activity',
    'dwUserTable': 'user_detail',
    'dwCourseTable': 'content',
    'dwEnrollmentsTable': 'user_enrolments',
    'dwUnenrollmentsTable': 'unenrolled_user_audit',
    'dwOrgTable': 'org_hierarchy',
    'dwAssessmentTable': 'assessment_detail',
    'dwBPEnrollmentsTable': 'bp_enrolments',
    'dwKcmDictionaryTable': 'kcm_dictionary',
    'dwKcmContentTable': 'kcm_content_mapping',
    'dwCBPlanTable': 'cb_plan',
    'dwContentResourceTable': 'content_resource',
    'dwEventsTable': 'events',
    'dwEventsEnrolmentTable': 'events_enrolment',
    'dwLearnerStatsTable': 'learner_stats',
    'dwSLWMdoLeaderboardTable': 'slw_mdo_leaderboard',
    'dwSLWMdoTopLearnerTable': 'slw_mdo_top_learners',
    'dwNLWUserLeaderboardTable': 'nlw_user_leaderboard',
    'dwAparCBPEnrollmentTable': 'apar_cbp_enrollment',
    'dwCourseCompletionSurveryTable': 'course_completion_survey_details',
    'dwBharatKalpCoursesTable': 'bk_course_enrolments',
    'dwBharatKalpEventsTable': 'bk_event_enrolments',
    'dwpeerValidationNotificationQueue': 'peer_validation_notification_queue',
    'dwpeerValidationFormStateTable': 'peer_validation_form_state',

    # Clickhouse Data Warehouse
    'dwClickhouseUsername': 'pyspark',
    'dwClickhouseSchema': 'warehouse',
    'dwUserTable': 'user_detail',
    'dwCourseTable': 'content',
    'dwEnrollmentsTable': 'user_enrolments',
    'dwOrgTable': 'org_hierarchy',
    'dwAssessmentTable': 'assessment_detail',
    'dwBPEnrollmentsTable': 'bp_enrolments',
    'dwKcmDictionaryTable': 'kcm_dictionary',
    'dwKcmContentTable': 'kcm_content_mapping',
    'dwCBPlanTable': 'cb_plan',
    'dwContentResourceTable': 'content_resource',
    'dwEventsTable': 'events',
    'dwEventsEnrolmentTable': 'events_enrolment',
    'dwCourseCompletionSurveryTable': 'course_completion_survey_details',
    'dwUserEnrolmentsCustomTable': 'user_enrolments_custom',
    'dwContentCustomTable': 'content_custom',
    'dwMyIgotTable': 'myigot',
    'dwLOrgHierarchyTable': 'L_org_hierarchy',
    'dwMdoEnrolmentFactTable': 'mdo_enrolment_fact',
    'dwMdoUserSummaryTable': 'mdo_user_summary',
    'dwAiCbpFactTable': 'ai_cbp_fact',                                       
    'dwLearnerCoverageMapTable': 'learner_coverage_map',                     
    'dwFunnelStageFactTable': 'funnel_stage_fact',                           
    'dwRecommendedCourseCompetencyMapTable': 'recommended_course_competency_map',   
    'dwSavedCourseMapTable': 'saved_course_map',                             
    'dwOrgDeptUserSummaryTable': 'org_dept_user_summary',                    
    'dwCompetencyMasterListTable': 'competency_master_list',                 
    'dwUserAccessTable': 'user_access',                                      
    'dwOrgAccessTable': 'org_access', 

    # Cassandra Keyspaces
    'cassandraUserKeyspace': 'sunbird',
    'cassandraCourseKeyspace': 'sunbird_courses',
    'cassandraHierarchyStoreKeyspace': 'dev_hierarchy_store',
    'cassandraUserFeedKeyspace': 'sunbird_notifications',

    # Cassandra Core Host
    'lpCassandraHost': '192.168.3.211',

    # Cassandra Tables
    'cassandraUserTable': 'user',
    'cassandraUserRolesTable': 'user_roles',
    'cassandraOrgTable': 'organisation',
    'cassandraUserEnrolmentsTable': 'user_enrolments_v2',
    'cassandraContentHierarchyTable': 'content_hierarchy',
    'cassandraRatingSummaryTable': 'ratings_summary',
    'cassandraRatingsTable': 'ratings',
    'cassandraOrgHierarchyTable': 'org_hierarchy',
    'cassandraCourseBatchTable': 'course_batch',
    'cassandraLearnerStatsTable': 'learner_stats',
    'cassandraKarmaPointsTable': 'user_karma_points',
    'cassandraHallOfFameTable': 'mdo_karma_points',
    'cassandraUserAssessmentTable': 'user_assessment_data_v2',
    'cassandraKarmaPointsLookupTable': 'user_karma_points_credit_lookup',
    'cassandraKarmaPointsSummaryTable': 'user_karma_points_summary',
    'cassandraUserFeedTable': 'notification_feed',
    'cassandraAcbpTable': 'cb_plan_v4',
    'cassandraUserGroupInfoTable': 'user_group_info',
    'cassandraLearnerLeaderBoardLookupTable': 'learner_leaderboard_lookup',
    'cassandraLearnerLeaderBoardTable': 'learner_leaderboard',
    'cassandraOldAssesmentTable': 'user_assessment_master',
    'cassandraNLWMdoLeaderboardTable': 'nlw_mdo_leaderboard',
    'cassandraNLWUserLeaderboardTable': 'nlw_user_leaderboard',
    'cassandraPublicUserAssessmentDataTable': 'public_user_assessment_data',
    'cassandraUserEntityEnrolmentTable': 'user_entity_enrolments',
    'cassandraFrameworkHierarchyTable': 'framework_hierarchy',
    'cassandraGroupDesignationTable': 'kb_group_designation_content_data',
    'cassandraMDOLearnerLeaderboardTable': 'mdo_learner_leaderboard',
    'cassandraSLWMdoLeaderboardTable': 'slw_mdo_leaderboard',
    'cassandraSLWMdoTopLearnerTable': 'slw_mdo_top_learners',
    'cassandraUserExtendedProfileTable' : "user_extended_profile",
    'cassandraQuestionSetHierarchyTable': "questionset_hierarchy",
    'cassandraAccessSettingRulesTable': "access_setting_rules_v2",
    # MongoDB Configuration
    'mongoDatabase': 'nodebb',
    'mongoDBCollection': 'objects',

    # Storage Configuration
    'key': 'aws_storage_key',
    'secret': 'aws_storage_secret',
    'store': 'gs',
    'container': 'igot',
    'bucket': 'igot',
    'storageKeyConfig': 'storage.key.config',
    'storageSecretConfig': 'storage.secret.config',
    'dpRawTelemetryBackupLocation': 'raw-telemetry-backup',

    # Survey and Assessment Configuration
    'platformRatingSurveyId': '1696404440829',
    'cutoffTime': '60.0',
    'reportSyncEnable': 'true',
    'mdoIDs': '',
    'anonymousAssessmentLoggedInUserContentIDs': 'do_1141533540853432321675,do_1141533857591132161321,do_1141525365329264641663,do_1141527106280980481664',
    'anonymousAssessmentNonLoggedInUserAssessmentIDs': 'do_11415336159226265611',
    'hardcodeTrendingCourses': True,
    'hardCodedCoursesIds': "do_1141142234379386881387,do_1143613347908812801129,do_1144314741016166401174,do_1143089865482649601691,do_114125479290847232144",
    # Report Paths
    'userReportPath': 'standalone-reports/user-report',
    'userEnrolmentReportPath': 'standalone-reports/user-enrollment-report',
    'userUnenrolmentReportPath': 'standalone-reports/user-unenrollment-report',
    'courseReportPath': 'standalone-reports/course-report',
    'cbaReportPath': 'standalone-reports/cba-report',
    'taggedUsersPath': 'tagged-users/',
    'standaloneAssessmentReportPath': 'standalone-reports/user-assessment-report-cbp',
    'blendedReportPath': 'standalone-reports/blended-program-report',
    'orgHierarchyReportPath': 'standalone-reports/org-hierarchy-report',
    'acbpReportPath': 'standalone-reports/cbp-report',
    'acbpMdoEnrolmentReportPath': 'standalone-reports/cbp-report-mdo-enrolment',
    'acbpMdoSummaryReportPath': 'standalone-reports/cbp-report-mdo-summary',
    'kcmReportPath': 'standalone-reports/kcm-report',
    'validationReportPath': 'standalone-reports/validation-report',
    'courseCompletionSurveyPath': 'standalone-reports/course-completion-survey-report',
    'gamificationReportPath' : 'standalone-reports/gamification-report',

    'blendedProgramReport' : 'BlendedProgramReport.csv',
    'cbaReport' : 'UserAssessmentReport.csv',
    'cbpEnrolmentReport' : 'CBPEnrollmentReport.csv',
    'cbpSummaryReport' : 'CBPUserSummaryReport.csv',
    'courseReport' : 'ContentReport.csv',
    'kcmReport' : 'ContentCompetencyMapping.csv',
    'userAssessmentReport' : 'StandaloneAssessmentReport.csv',
    'userEnrollmentReport' : 'ConsumptionReport.csv',
    'userUnenrolmentReport' : 'UnenrollmentReport.csv',
    'userReport' : 'UserReport.csv',
    'completionSurveyReport':'completionSurvey.csv',

    # Elasticsearch Configuration
    'esFormDataIds': '1718964921012,1720793361489',
    'esFormDataIndex': 'form_data',
    'completionSurveyFormIds':["1766408457687","1766407740425"],
    'contentEndSurveyFormid' : ['1766408457687'],

    # Learning Week Configuration
    'nationalLearningWeekStart': '2024-10-19 00:00:00',
    'nationalLearningWeekEnd': '2024-10-27 23:59:59',
    'stateLearningWeekStart': '2024-10-19 00:00:00',
    'stateLearningWeekEnd': '2025-03-14 23:59:59',
    'sizeBucketString': '1-1000-XS,1001-2500-S,2501-5000-M,5001-10000-L,10001-50000-XL,above 50000-XXL',
    'stateSizeBucketString' : '1-100000-S,100001-500000-M,above 500000-L',
    'nlwStatesList': 'ANDAMAN and NICOBAR,ANDHRA PRADESH,ARUNACHAL PRADESH,ASSAM,BIHAR,CHANDIGARH,CHHATTISGARH,Dadra and Nagar Haveli and Daman and Diu,DELHI,GOA,GUJARAT,HARYANA,HIMACHAL PRADESH,JAMMU and KASHMIR,JHARKHAND,KARNATAKA,KERALA,LADAKH,LAKSHADWEEP,MADHYA PRADESH,MAHARASHTRA,MANIPUR,MEGHALAYA,MIZORAM,NAGALAND,ODISHA,PUDUCHERRY,PUNJAB,RAJASTHAN,SIKKIM,TAMIL NADU,TELANGANA,TRIPURA,UTTAR PRADESH,UTTARAKHAND,WEST BENGAL',
    'stateUniverseMap': {'CHANDIGARH': 37000,'CHHATTISGARH': 377881,'DELHI': 80000,'GUJARAT': 450000,'HIMACHAL PRADESH': 180000,'JAMMU and KASHMIR': 400000,'JHARKHAND': 150000,'KERALA': 600000,'LADAKH': 12000,'LAKSHADWEEP': 9600,'ODISHA': 400000,'PUDUCHERRY': 25000,'PUNJAB': 350000,'RAJASTHAN': 750000,'TAMIL NADU': 1500000,'TELANGANA': 500000,'UTTAR PRADESH': 2000000,'UTTARAKHAND': 177579,'WEST BENGAL': 1000000},
    'overridesForSlw': {},
    'rollupRequiredOrgs': [],

    # Zip Reports Configuration
    'prefixDirectoryPath': 'standalone-reports',
    'destinationDirectoryPath': 'standalone-reports/merged',
    'localReportDir': f'{BASE_DIR}/reports',
    'warehouseReportDir': f'{BASE_DIR}/warehouse',
    'baseCachePath': f'{BASE_DIR}/data-res/pq_files/cache_pq/',
    'bqScriptPath': f'{BASE_DIR}/bq-scripts.sh',
    'warehouseOutputDir': f'{BASE_DIR}/warehouse/fullReport/',
    'mdoReportSyncPath': 'standalone-reports/merged/',
    'fullReportSyncPath': 'standalone-reports/merged/fullReport/',
    'kcmSyncPath': 'standalone-reports/merged/kcm/',
    'unifiedParquetPath': 'airflowData/',
    'unifiedParquetLocalPath': '/home/analytics/pyspark/warehouse/unified/',
    'directoriesToSelect': ["blended-program-report-mdo","cbp-report-mdo-summary","course-report","cba-report","cbp-report-mdo-enrolment","user-report","user-enrollment-report"],
    'pysparkDirectoriesToSelect': ["blended-program-report-mdo","cbp-report-mdo-summary","gamification-report","course-report","cba-report","cbp-report-mdo-enrolment","user-report","user-enrollment-report"],
    'pysparkCBPDirectoriesToSelect': ["user-custom-report","blended-program-report-cbp","user-assessment-report-cbp", "course-completion-survey-report"],
    'googleServiceAccountFilePath': '/home/analytics/pyspark/jobs/gcp_service_account.json',
    'gcpBucket': 'igotproddp',
    'password': '123456',
    'createFullReport': False,
    'warehouseUserCustomReportDir': 'user_custom_report',
    'password_length': 6,

    # Job Configuration
    'parallelization': '16',
    'parallelizationSmall': '8',
    'modelParamsParallelization': '200',
    'apiVersion': 'v2',
    'deviceMapping': False,
    'reportSyncEnableSL': 'true',
    'reportZipSyncEnable': 'true',

    # Kafka/Messaging Configuration
    'brokerList': '192.168.3.249:9092',
    'topic': 'dev.dashboard.default',
    'compression': 'none',

    # Peer validation configuration
    'notificationBatchSize': 100,
    'apiBasedNotificationEnabled': True,
    'notificationAPIURL': 'http://10.175.5.200/cb-notification/v1/notifications/bulk/create/peervalidation',
    'peerValidationKafkaTopic':'prod.peer.survey.notification.sent',
    'peerValidationFormIndex': 'fs-forms-alias-v2',

    # Course category configuration
    'courseCategoriesToSelect': ["Comprehensive Assessment", "Course", "Moderated Course", "Invite-Only Program", "Moderated Program", "Blended Program", "Curated Program", "Standalone Assessment", "Moderated Assessment", "Invite-Only Assessment", "External Redirect", "Case Study", "Comprehensive Assessment Program", "Multilingual Course", "Pre Enrolment Assessment", "Learning Pathway"],

    # Bharat Kalp Configuration
    'bharat_kalp_event_tags': ["Bharat Kalp - Talks", "Bharat Kalp - Podcast"],
    'bharatKalpCoursesApiUrl' : "https://portal.dev.karmayogibharat.net/apis/static/form/v1/read"

}

# Side Output Configuration (from Scala sideOutput map)
DEFAULT_SIDE_OUTPUT = {
    'brokerList': '192.168.3.249:9092',
    'compression': 'none',
    'topics': {
        'roleUserCount': 'dev.dashboards.role.count',
        'orgRoleUserCount': 'dev.dashboards.org.role.count',
        'allCourses': 'dev.dashboards.course',
        'userCourseProgramProgress': 'dev.dashboards.user.course.program.progress',
        'fracCompetency': 'dev.dashboards.competency.frac',
        'courseCompetency': 'dev.dashboards.competency.course',
        'expectedCompetency': 'dev.dashboards.competency.expected',
        'declaredCompetency': 'dev.dashboards.competency.declared',
        'competencyGap': 'dev.dashboards.competency.gap',
        'userOrg': 'dev.dashboards.user.org',
        'org': 'dev.dashboards.org',
        'userAssessment': 'dev.dashboards.user.assessment',
        'assessment': 'dev.dashboards.assessment'
    }
}

# Add sideOutput to DEFAULT_CONFIG
DEFAULT_CONFIG['sideOutput'] = DEFAULT_SIDE_OUTPUT


class SimpleConfig:
    def __init__(self, config_dict, defaults=None):
        # Set defaults first
        if defaults:
            for key, value in defaults.items():
                setattr(self, key, value)

        # Override with actual config values
        for key, value in config_dict.items():
            # SIMPLE FIX: If value has {{ }} and we're local, use default instead
            if isinstance(value, str) and '{{' in value and '/Users/' in os.getcwd():
                # Use default value instead of template
                if hasattr(self, key):
                    # print(f"[LOCAL] Using default for {key}: {getattr(self, key)}")
                    continue  # Keep the default value, don't override

            setattr(self, key, value)

    def get(self, key, default=None):
        """Backward compatibility with .get() method"""
        return getattr(self, key, default)


def create_config(config_dict):
    """Create a SimpleConfig instance with default values"""
    return SimpleConfig(config_dict, DEFAULT_CONFIG)
