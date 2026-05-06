"""
AWS Lake Formation + Glue Integration PoC — CDK Python Stack
=============================================================
Resources provisioned
  • S3 data lake bucket  (raw / cleaned / curated prefixes)
  • S3 Athena results bucket
  • S3 Glue scripts bucket
  • IAM roles  (LakeFormation service role, Glue job role, two analyst users)
  • Lake Formation admin + data location registration
  • Glue Database (poc_database)          — aws_glue_alpha.Database   (L2)
  • Glue Crawler  (raw CSV → Glue Catalog) — glue.CfnCrawler           (L1, no L2 exists)
  • Glue ETL Job  (CSV → Parquet)          — aws_glue_alpha.Job        (L2)
  • Lake Formation permissions             — lf.CfnPermissions         (L1, no L2 exists)
      - Glue role: CREATE_TABLE + DATA_LOCATION_ACCESS
      - analyst_alice: SELECT on all columns
      - analyst_bob:   SELECT on restricted columns (no SSN)
  • LF Tag  sensitivity=public|internal|confidential
  • Athena WorkGroup                       — athena.CfnWorkGroup       (L1, no L2 exists)
"""

import json
import os
from aws_cdk import (
    Stack,
    CfnOutput,
    RemovalPolicy,
    Duration,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_iam as iam,
    aws_glue as glue,
    aws_lakeformation as lf,
    aws_athena as athena,
    aws_events as events,
    aws_events_targets as targets,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
import aws_cdk.aws_glue_alpha as glue_alpha
from aws_cdk import DefaultStackSynthesizer
from constructs import Construct
from lake_formation_cdk.config import (
    LF_SERVICE_ROLE_NAME,
    GLUE_JOB_ROLE_NAME,
    ANALYST_ALICE_NAME,
    ANALYST_BOB_NAME,
    GLUE_DATABASE_NAME,
    CRAWLER_NAME,
    GLUE_JOB_NAME,
    ROW_FILTER_NAME,
    ATHENA_WORKGROUP_NAME,
    EVENTBRIDGE_RULE_NAME,
    STATE_MACHINE_NAME,
    data_lake_bucket_name,
    athena_results_bucket_name,
    glue_scripts_bucket_name,
)


class LakeFormationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account = self.account
        region = self.region

        # ──────────────────────────────────────────────────────────────────────
        # 1.  S3 BUCKETS
        # ──────────────────────────────────────────────────────────────────────
        data_lake_bucket = s3.Bucket(
            self,
            "DataLakeBucket",
            bucket_name=data_lake_bucket_name(account, region),
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            event_bridge_enabled=True,
        )

        athena_results_bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            bucket_name=athena_results_bucket_name(account, region),
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        glue_scripts_bucket = s3.Bucket(
            self,
            "GlueScriptsBucket",
            bucket_name=glue_scripts_bucket_name(account, region),
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Upload sample CSV and ETL script
        assets_dir = os.path.join(os.path.dirname(__file__), "../assets")

        s3deploy.BucketDeployment(
            self,
            "UploadSampleData",
            sources=[s3deploy.Source.asset(os.path.join(assets_dir, "data"))],
            destination_bucket=data_lake_bucket,
            destination_key_prefix="raw/customers/year=2026/month=04/",
            prune=False,
        )

        s3deploy.BucketDeployment(
            self,
            "UploadGlueScript",
            sources=[s3deploy.Source.asset(os.path.join(assets_dir, "scripts"))],
            destination_bucket=glue_scripts_bucket,
            destination_key_prefix="scripts/",
        )

        # ──────────────────────────────────────────────────────────────────────
        # 2.  IAM ROLES
        # ──────────────────────────────────────────────────────────────────────

        # ── 2a. Lake Formation service role ───────────────────────────────────
        lf_service_role = iam.Role(
            self,
            "LakeFormationServiceRole",
            role_name=LF_SERVICE_ROLE_NAME,
            assumed_by=iam.ServicePrincipal("lakeformation.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AWSLakeFormationDataAdmin"
                )
            ],
        )
        lf_service_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
                resources=[
                    data_lake_bucket.bucket_arn,
                    f"{data_lake_bucket.bucket_arn}/*",
                ],
            )
        )

        # ── 2b. Glue crawler + ETL job role ───────────────────────────────────
        glue_role = iam.Role(
            self,
            "GlueJobRole",
            role_name=GLUE_JOB_ROLE_NAME,
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSLakeFormationDataAdmin"),
            ],
        )
        # S3 access for the Glue role
        data_lake_bucket.grant_read_write(glue_role)
        glue_scripts_bucket.grant_read(glue_role)
        athena_results_bucket.grant_read_write(glue_role)

        # Glue needs lakeformation:GetDataAccess to get temporary credentials
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lakeformation:GetDataAccess"],
                resources=["*"],
            )
        )

        # # ── 2c. Data engineer user (full access) ──────────────────────────────
        # analyst_alice = iam.User(
        #     self,
        #     "AnalystAlice",
        #     user_name=ANALYST_ALICE_NAME,
        #     password=cdk_secret_value("AliceP@ssw0rd!"),
        # )
        # analyst_alice.add_managed_policy(
        #     iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess")
        # )
        # analyst_alice.add_to_policy(
        #     iam.PolicyStatement(
        #         actions=["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
        #         resources=[
        #             athena_results_bucket.bucket_arn,
        #             f"{athena_results_bucket.bucket_arn}/*",
        #             data_lake_bucket.bucket_arn,
        #             f"{data_lake_bucket.bucket_arn}/*",
        #         ],
        #     )
        # )
        # analyst_alice.add_to_policy(
        #     iam.PolicyStatement(
        #         actions=["lakeformation:GetDataAccess"],
        #         resources=["*"],
        #     )
        # )

        # ── 2d. Restricted analyst (no SSN column) ────────────────────────────
        # analyst_bob = iam.User(
        #     self,
        #     "AnalystBob",
        #     user_name=ANALYST_BOB_NAME,
        #     password=cdk_secret_value("BobP@ssw0rd!"),
        # )
        # analyst_bob.add_managed_policy(
        #     iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess")
        # )
        # analyst_bob.add_to_policy(
        #     iam.PolicyStatement(
        #         actions=["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
        #         resources=[
        #             athena_results_bucket.bucket_arn,
        #             f"{athena_results_bucket.bucket_arn}/*",
        #             data_lake_bucket.bucket_arn,
        #             f"{data_lake_bucket.bucket_arn}/*",
        #         ],
        #     )
        # )
        # analyst_bob.add_to_policy(
        #     iam.PolicyStatement(
        #         actions=["lakeformation:GetDataAccess"],
        #         resources=["*"],
        #     )
        # )

        # ──────────────────────────────────────────────────────────────────────
        # 3.  LAKE FORMATION ADMIN  +  DATA LOCATION
        # ──────────────────────────────────────────────────────────────────────

        # CfnDataLakeSettings REPLACES the entire admin list, so the CDK bootstrap
        # CloudFormation execution role must be included. Without it, subsequent LF
        # resources (CfnResource, CfnPermissions) are rejected with "Invalid principal"
        # because the role performing the deployment is no longer an LF admin.
        # Reads the qualifier from CDK context (set when using --qualifier at bootstrap)
        # and falls back to the CDK default so custom bootstrap qualifiers are handled.
        qualifier = (
            self.node.try_get_context("@aws-cdk/core:bootstrapQualifier")
            or DefaultStackSynthesizer.DEFAULT_QUALIFIER
        )
        cfn_exec_role_arn = (
            f"arn:aws:iam::{account}:role/"
            f"cdk-{qualifier}-cfn-exec-role-{account}-{region}"
        )

        lf_settings = lf.CfnDataLakeSettings(
            self,
            "LakeFormationSettings",
            admins=[
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=lf_service_role.role_arn
                ),
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=glue_role.role_arn
                ),
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=cfn_exec_role_arn
                ),
            ],
        )

        # Register the S3 data lake location with Lake Formation
        lf_location = lf.CfnResource(
            self,
            "DataLakeLocation",
            resource_arn=data_lake_bucket.bucket_arn,
            use_service_linked_role=False,
            role_arn=lf_service_role.role_arn,
        )
        lf_location.add_dependency(lf_settings)

        # ──────────────────────────────────────────────────────────────────────
        # 4.  GLUE DATABASE  (L2 — aws_glue_alpha.Database)
        # ──────────────────────────────────────────────────────────────────────
        glue_db = glue_alpha.Database(
            self,
            "PocDatabase",
            database_name=GLUE_DATABASE_NAME,
            description="Lake Formation + Glue PoC database",
            location_uri=f"s3://{data_lake_bucket.bucket_name}/",
        )

        # ──────────────────────────────────────────────────────────────────────
        # 5.  LAKE FORMATION PERMISSIONS  (Glue role on database + location)
        #     aws_lakeformation has no L2/L3 — CfnPermissions is the only option.
        # ──────────────────────────────────────────────────────────────────────

        # Glue role → CREATE_TABLE + ALTER on poc_database
        glue_role_db_perm = lf.CfnPermissions(
            self,
            "GlueRoleDbPermissions",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=glue_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=account,
                    name=GLUE_DATABASE_NAME,
                )
            ),
            permissions=["CREATE_TABLE", "ALTER", "DESCRIBE"],
        )
        glue_role_db_perm.node.add_dependency(glue_db)

        # Glue role → DATA_LOCATION_ACCESS on S3 bucket
        lf.CfnPermissions(
            self,
            "GlueRoleLocationPermissions",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=glue_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                data_location_resource=lf.CfnPermissions.DataLocationResourceProperty(
                    catalog_id=account,
                    s3_resource=data_lake_bucket.bucket_arn,
                )
            ),
            permissions=["DATA_LOCATION_ACCESS"],
        ).add_dependency(lf_location)

        # ──────────────────────────────────────────────────────────────────────
        # 6.  GLUE CRAWLER  (L1 — no L2/L3 exists for CfnCrawler)
        # ──────────────────────────────────────────────────────────────────────
        glue_crawler = glue.CfnCrawler(
            self,
            "RawCustomersCrawler",
            name=CRAWLER_NAME,
            role=glue_role.role_arn,
            database_name=GLUE_DATABASE_NAME,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{data_lake_bucket.bucket_name}/raw/customers/",
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
            configuration=json.dumps(
                {
                    "Version": 1.0,
                    "CrawlerOutput": {
                        "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
                        "Tables": {"AddOrUpdateBehavior": "MergeNewColumns"},
                    },
                    "Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas"},
                }
            ),
            description="Crawls raw CSV customer data and registers schema in Glue Catalog",
        )
        glue_crawler.node.add_dependency(glue_db)

        # ──────────────────────────────────────────────────────────────────────
        # 7.  STEP FUNCTIONS — INGEST PIPELINE
        #     Triggered by EventBridge on S3 ObjectCreated under raw/customers/.
        #     Flow: StartCrawler → poll until READY → check result → RunEtlJob.
        # ──────────────────────────────────────────────────────────────────────

        # 1. Start the crawler
        start_crawler_task = sfn_tasks.CallAwsService(
            self, "StartCrawler",
            service="glue",
            action="startCrawler",
            parameters={"Name": CRAWLER_NAME},
            iam_resources=["*"],
            result_path=sfn.JsonPath.DISCARD,
        )

        # 2. Wait before polling
        wait_for_crawler = sfn.Wait(
            self, "WaitForCrawler",
            time=sfn.WaitTime.duration(Duration.seconds(30)),
        )

        # 3. Poll crawler state
        get_crawler_status = sfn_tasks.CallAwsService(
            self, "GetCrawlerStatus",
            service="glue",
            action="getCrawler",
            parameters={"Name": CRAWLER_NAME},
            iam_resources=["*"],
            result_path="$.CrawlerStatus",
        )

        # 4. Terminal states
        crawler_failed = sfn.Fail(
            self, "CrawlerFailed",
            cause="Glue crawler did not succeed",
        )

        # 5. Run ETL job — RUN_JOB integration waits for completion
        run_etl_job = sfn_tasks.GlueStartJobRun(
            self, "RunEtlJob",
            glue_job_name=GLUE_JOB_NAME,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        # 6. Branch on crawler result (READY → check LastCrawl.Status)
        check_crawl_result = sfn.Choice(self, "DidCrawlerSucceed?")
        check_crawl_result.when(
            sfn.Condition.string_equals(
                "$.CrawlerStatus.Crawler.LastCrawl.Status", "SUCCEEDED"
            ),
            run_etl_job,
        ).otherwise(crawler_failed)

        # 7. Loop back while crawler is still running
        is_crawler_running = sfn.Choice(self, "IsCrawlerRunning?")
        is_crawler_running.when(
            sfn.Condition.or_(
                sfn.Condition.string_equals("$.CrawlerStatus.Crawler.State", "RUNNING"),
                sfn.Condition.string_equals("$.CrawlerStatus.Crawler.State", "STOPPING"),
            ),
            wait_for_crawler,
        ).otherwise(check_crawl_result)

        # Wire up the chain
        pipeline_definition = (
            start_crawler_task
            .next(wait_for_crawler)
            .next(get_crawler_status)
            .next(is_crawler_running)
        )

        pipeline = sfn.StateMachine(
            self, "IngestPipeline",
            state_machine_name=STATE_MACHINE_NAME,
            definition_body=sfn.DefinitionBody.from_chainable(pipeline_definition),
        )

        # EventBridge fires the state machine when a new object lands in raw/customers/
        new_raw_data_rule = events.Rule(
            self,
            "NewRawDataRule",
            rule_name=EVENTBRIDGE_RULE_NAME,
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [data_lake_bucket.bucket_name]},
                    "object": {"key": [{"prefix": "raw/customers/"}]},
                },
            ),
        )
        new_raw_data_rule.add_target(targets.SfnStateMachine(pipeline))

        # ──────────────────────────────────────────────────────────────────────
        # 8.  GLUE ETL JOB  (L2 — aws_glue_alpha.Job)
        # ──────────────────────────────────────────────────────────────────────
        glue_job = glue_alpha.PySparkEtlJob(
            self,
            "CsvToParquetJob",
            job_name=GLUE_JOB_NAME,
            role=glue_role,
            script=glue_alpha.Code.from_bucket(
                glue_scripts_bucket,
                "scripts/etl_csv_to_parquet.py",
            ),
            glue_version=glue_alpha.GlueVersion.V4_0,
            number_of_workers=2,
            worker_type=glue_alpha.WorkerType.G_1X,
            timeout=Duration.minutes(30),
            max_retries=0,
            default_arguments={
                "--enable-glue-datacatalog": "true",
                "--job-bookmark-option": "job-bookmark-enable",
                "--SOURCE_DATABASE": GLUE_DATABASE_NAME,
                "--SOURCE_TABLE": "customers",
                "--CLEANED_PATH": f"s3://{data_lake_bucket.bucket_name}/cleaned/customers/",
                "--TARGET_PATH": f"s3://{data_lake_bucket.bucket_name}/curated/customers/",
                "--PARTITION_KEYS": "year,month",
                "--TARGET_DATABASE": GLUE_DATABASE_NAME,
                "--TARGET_TABLE": "curated_customers",
            },
            description="Transforms raw CSV customers to Parquet and writes to curated zone",
        )
        glue_job.node.add_dependency(glue_db)

        # ──────────────────────────────────────────────────────────────────────
        # 9.  LF-TAG  (sensitivity classification)
        #     aws_lakeformation has no L2/L3 — CfnTag is the only option.
        # ──────────────────────────────────────────────────────────────────────
        sensitivity_tag = lf.CfnTag(
            self,
            "SensitivityTag",
            tag_key="sensitivity",
            tag_values=["public", "internal", "confidential"],
        )
        sensitivity_tag.add_dependency(lf_settings)

        # ──────────────────────────────────────────────────────────────────────
        # 10.  LAKE FORMATION PERMISSIONS — ANALYSTS
        #     aws_lakeformation has no L2/L3 — CfnPermissions is the only option.
        # ──────────────────────────────────────────────────────────────────────

        # ── Alice: SELECT all columns on ALL tables in poc_database ───────────
        # alice_perm = lf.CfnPermissions(
        #     self,
        #     "AliceSelectAllTables",
        #     data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
        #         data_lake_principal_identifier=analyst_alice.user_arn
        #     ),
        #     resource=lf.CfnPermissions.ResourceProperty(
        #         table_resource=lf.CfnPermissions.TableResourceProperty(
        #             catalog_id=account,
        #             database_name=GLUE_DATABASE_NAME,
        #             table_wildcard=lf.CfnPermissions.TableWildcardProperty(),
        #         )
        #     ),
        #     permissions=["SELECT", "DESCRIBE"],
        # )
        # alice_perm.node.add_dependency(glue_db)

        # ── Bob: SELECT restricted columns only on raw customers table ─────────
        # Omits "ssn" — demonstrates column-level security
        # bob_perm = lf.CfnPermissions(
        #     self,
        #     "BobRestrictedColumns",
        #     data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
        #         data_lake_principal_identifier=analyst_bob.user_arn
        #     ),
        #     resource=lf.CfnPermissions.ResourceProperty(
        #         table_with_columns_resource=lf.CfnPermissions.TableWithColumnsResourceProperty(
        #             catalog_id=account,
        #             database_name=GLUE_DATABASE_NAME,
        #             name="customers",
        #             column_names=[
        #                 "customer_id",
        #                 "first_name",
        #                 "last_name",
        #                 "email",
        #                 "region",
        #                 "signup_date",
        #                 "annual_spend",
        #                 # "ssn" intentionally excluded
        #             ],
        #         )
        #     ),
        #     permissions=["SELECT", "DESCRIBE"],
        # )
        # bob_perm.node.add_dependency(glue_db)

        # # ── Bob: Row-level filter — only us-east-1 rows on curated table ───────
        # bob_row_filter = lf.CfnDataCellsFilter(
        #     self,
        #     "BobRowFilter",
        #     name=ROW_FILTER_NAME,
        #     database_name=GLUE_DATABASE_NAME,
        #     table_name="curated_customers",
        #     table_catalog_id=account,
        #     row_filter=lf.CfnDataCellsFilter.RowFilterProperty(
        #         filter_expression="region = 'us-east-1'"
        #     ),
        #     column_wildcard=lf.CfnDataCellsFilter.ColumnWildcardProperty(),
        # )
        # bob_row_filter.node.add_dependency(glue_db)

        # ──────────────────────────────────────────────────────────────────────
        # 11.  ATHENA WORKGROUP
        #      aws_athena has no L2/L3 — CfnWorkGroup is the only option.
        # ──────────────────────────────────────────────────────────────────────
        athena.CfnWorkGroup(
            self,
            "PocWorkGroup",
            name=ATHENA_WORKGROUP_NAME,
            description="Workgroup for Lake Formation PoC Athena queries",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=(
                        f"s3://{athena_results_bucket.bucket_name}/results/"
                    ),
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                ),
            ),
            state="ENABLED",
            recursive_delete_option=True,
        )

        # ──────────────────────────────────────────────────────────────────────
        # 12.  CFN OUTPUTS
        # ──────────────────────────────────────────────────────────────────────
        CfnOutput(self, "DataLakeBucketName",
                  value=data_lake_bucket.bucket_name,
                  description="S3 data lake bucket")
        CfnOutput(self, "GlueScriptsBucketName",
                  value=glue_scripts_bucket.bucket_name,
                  description="Glue scripts bucket")
        CfnOutput(self, "AthenaResultsBucketName",
                  value=athena_results_bucket.bucket_name,
                  description="Athena query results bucket")
        CfnOutput(self, "GlueJobName",
                  value=glue_job.job_name,
                  description="Glue ETL job name — run after the crawler completes")
        CfnOutput(self, "CrawlerName",
                  value=glue_crawler.ref,
                  description="Glue crawler — run this first to populate the catalog")
        CfnOutput(self, "GlueRoleArn",
                  value=glue_role.role_arn,
                  description="IAM role used by Glue and registered as LF admin")
        # CfnOutput(self, "AliceUserArn",
        #           value=analyst_alice.user_arn,
        #           description="Analyst Alice — full column access")
        # CfnOutput(self, "BobUserArn",
        #           value=analyst_bob.user_arn,
        #           description="Analyst Bob — restricted columns + us-east-1 rows only")
        CfnOutput(self, "AthenaWorkgroup",
                  value=ATHENA_WORKGROUP_NAME,
                  description="Use this Athena workgroup for all PoC queries")
        CfnOutput(self, "SampleAthenaQuery",
                  value=f"SELECT * FROM {GLUE_DATABASE_NAME}.customers LIMIT 10;",
                  description="Run as Alice (all columns) vs Bob (ssn column hidden)")


# Helper — avoids importing SecretValue at the top level
def cdk_secret_value(plain: str):
    from aws_cdk import SecretValue
    return SecretValue.unsafe_plain_text(plain)
