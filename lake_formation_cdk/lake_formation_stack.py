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
    aws_lambda as lambda_,
)
import aws_cdk.aws_glue_alpha as glue_alpha
from constructs import Construct
from lake_formation_cdk.config import (
    PREFIX,
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
            role_name=f"{PREFIX}-lf-service-role",
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
            role_name=f"{PREFIX}-glue-job-role",
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

        # ── 2c. Data engineer user (full access) ──────────────────────────────
        analyst_alice = iam.User(
            self,
            "AnalystAlice",
            user_name=f"{PREFIX}-analyst-alice",
            password=cdk_secret_value("AliceP@ssw0rd!"),
        )
        analyst_alice.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess")
        )
        analyst_alice.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
                resources=[
                    athena_results_bucket.bucket_arn,
                    f"{athena_results_bucket.bucket_arn}/*",
                    data_lake_bucket.bucket_arn,
                    f"{data_lake_bucket.bucket_arn}/*",
                ],
            )
        )
        analyst_alice.add_to_policy(
            iam.PolicyStatement(
                actions=["lakeformation:GetDataAccess"],
                resources=["*"],
            )
        )

        # ── 2d. Restricted analyst (no SSN column) ────────────────────────────
        analyst_bob = iam.User(
            self,
            "AnalystBob",
            user_name=f"{PREFIX}-analyst-bob",
            password=cdk_secret_value("BobP@ssw0rd!"),
        )
        analyst_bob.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess")
        )
        analyst_bob.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
                resources=[
                    athena_results_bucket.bucket_arn,
                    f"{athena_results_bucket.bucket_arn}/*",
                    data_lake_bucket.bucket_arn,
                    f"{data_lake_bucket.bucket_arn}/*",
                ],
            )
        )
        analyst_bob.add_to_policy(
            iam.PolicyStatement(
                actions=["lakeformation:GetDataAccess"],
                resources=["*"],
            )
        )

        # ──────────────────────────────────────────────────────────────────────
        # 3.  LAKE FORMATION ADMIN  +  DATA LOCATION
        # ──────────────────────────────────────────────────────────────────────

        # Register CDK execution role + Glue role as LF admins
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
                # Add your deployment IAM principal here if needed:
                # lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                #     data_lake_principal_identifier=f"arn:aws:iam::{account}:root"
                # ),
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
            database_name=f"{PREFIX}_database",
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
                    name=f"{PREFIX}_database",
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
            name=f"{PREFIX}-raw-customers-crawler",
            role=glue_role.role_arn,
            database_name=f"{PREFIX}_database",
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
        # 7.  S3 EVENT → EVENTBRIDGE → LAMBDA → START CRAWLER
        #     Fires the crawler automatically whenever a new object lands under
        #     raw/customers/.  The Lambda is minimal — it just calls StartCrawler.
        # ──────────────────────────────────────────────────────────────────────
        crawler_trigger_fn = lambda_.Function(
            self,
            "CrawlerTriggerFn",
            function_name=f"{PREFIX}-crawler-trigger",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_inline(
                "import boto3, os\n"
                "def handler(e, c):\n"
                "    boto3.client('glue').start_crawler(Name=os.environ['CRAWLER_NAME'])\n"
            ),
            environment={"CRAWLER_NAME": f"{PREFIX}-raw-customers-crawler"},
        )
        crawler_trigger_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:StartCrawler"],
                resources=["*"],
            )
        )

        new_raw_data_rule = events.Rule(
            self,
            "NewRawDataRule",
            rule_name=f"{PREFIX}-new-raw-data",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [data_lake_bucket.bucket_name]},
                    "object": {"key": [{"prefix": "raw/customers/"}]},
                },
            ),
        )
        new_raw_data_rule.add_target(targets.LambdaFunction(crawler_trigger_fn))

        # ──────────────────────────────────────────────────────────────────────
        # 8.  GLUE ETL JOB  (L2 — aws_glue_alpha.Job)
        # ──────────────────────────────────────────────────────────────────────
        glue_job = glue_alpha.Job(
            self,
            "CsvToParquetJob",
            job_name=f"{PREFIX}-etl",
            role=glue_role,
            executable=glue_alpha.JobExecutable.python_etl(
                glue_version=glue_alpha.GlueVersion.V4_0,
                python_version=glue_alpha.PythonVersion.THREE,
                script=glue_alpha.Code.from_bucket(
                    glue_scripts_bucket,
                    "scripts/etl_csv_to_parquet.py",
                ),
            ),
            worker_count=2,
            worker_type=glue_alpha.WorkerType.G_1_X,
            timeout=Duration.minutes(30),
            max_retries=0,
            default_arguments={
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-spark-ui": "true",
                "--enable-job-insights": "true",
                "--enable-glue-datacatalog": "true",
                "--job-bookmark-option": "job-bookmark-enable",
                "--SOURCE_DATABASE": f"{PREFIX}_database",
                "--SOURCE_TABLE": "customers",
                "--CLEANED_PATH": f"s3://{data_lake_bucket.bucket_name}/cleaned/customers/",
                "--TARGET_PATH": f"s3://{data_lake_bucket.bucket_name}/curated/customers/",
                "--PARTITION_KEYS": "year,month",
                "--TARGET_DATABASE": f"{PREFIX}_database",
                "--TARGET_TABLE": "curated_customers",
            },
            description="Transforms raw CSV customers to Parquet and writes to curated zone",
        )
        glue_job.node.add_dependency(glue_db)

        # Fires the ETL job automatically when the crawler finishes successfully.
        etl_trigger = glue.CfnTrigger(
            self,
            "EtlAfterCrawler",
            name=f"{PREFIX}-etl-trigger",
            type="CONDITIONAL",
            start_on_creation=True,
            actions=[glue.CfnTrigger.ActionProperty(job_name=glue_job.job_name)],
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        crawler_name=f"{PREFIX}-raw-customers-crawler",
                        crawl_state="SUCCEEDED",
                        logical_operator="EQUALS",
                    )
                ]
            ),
        )
        etl_trigger.node.add_dependency(glue_crawler)
        etl_trigger.node.add_dependency(glue_job.node.default_child)

        # ──────────────────────────────────────────────────────────────────────
        # 10.  LF-TAG  (sensitivity classification)
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
        # 11.  LAKE FORMATION PERMISSIONS — ANALYSTS
        #     aws_lakeformation has no L2/L3 — CfnPermissions is the only option.
        # ──────────────────────────────────────────────────────────────────────

        # ── Alice: SELECT all columns on ALL tables in poc_database ───────────
        alice_perm = lf.CfnPermissions(
            self,
            "AliceSelectAllTables",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=analyst_alice.user_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                table_resource=lf.CfnPermissions.TableResourceProperty(
                    catalog_id=account,
                    database_name=f"{PREFIX}_database",
                    table_wildcard=lf.CfnPermissions.TableWildcardProperty(),
                )
            ),
            permissions=["SELECT", "DESCRIBE"],
        )
        alice_perm.node.add_dependency(glue_db)

        # ── Bob: SELECT restricted columns only on raw customers table ─────────
        # Omits "ssn" — demonstrates column-level security
        bob_perm = lf.CfnPermissions(
            self,
            "BobRestrictedColumns",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=analyst_bob.user_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                table_with_columns_resource=lf.CfnPermissions.TableWithColumnsResourceProperty(
                    catalog_id=account,
                    database_name=f"{PREFIX}_database",
                    name="customers",
                    column_names=[
                        "customer_id",
                        "first_name",
                        "last_name",
                        "email",
                        "region",
                        "signup_date",
                        "annual_spend",
                        # "ssn" intentionally excluded
                    ],
                )
            ),
            permissions=["SELECT", "DESCRIBE"],
        )
        bob_perm.node.add_dependency(glue_db)

        # ── Bob: Row-level filter — only us-east-1 rows on curated table ───────
        bob_row_filter = lf.CfnDataCellsFilter(
            self,
            "BobRowFilter",
            name=f"{PREFIX}_bob_us_east_1_filter",
            database_name=f"{PREFIX}_database",
            table_name="curated_customers",
            table_catalog_id=account,
            row_filter=lf.CfnDataCellsFilter.RowFilterProperty(
                filter_expression="region = 'us-east-1'"
            ),
            column_wildcard=lf.CfnDataCellsFilter.ColumnWildcardProperty(),
        )
        bob_row_filter.node.add_dependency(glue_db)

        # ──────────────────────────────────────────────────────────────────────
        # 12.  ATHENA WORKGROUP
        #      aws_athena has no L2/L3 — CfnWorkGroup is the only option.
        # ──────────────────────────────────────────────────────────────────────
        athena.CfnWorkGroup(
            self,
            "PocWorkGroup",
            name=f"{PREFIX}-workgroup",
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
        # 13.  CFN OUTPUTS
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
        CfnOutput(self, "AliceUserArn",
                  value=analyst_alice.user_arn,
                  description="Analyst Alice — full column access")
        CfnOutput(self, "BobUserArn",
                  value=analyst_bob.user_arn,
                  description="Analyst Bob — restricted columns + us-east-1 rows only")
        CfnOutput(self, "AthenaWorkgroup",
                  value=f"{PREFIX}-workgroup",
                  description="Use this Athena workgroup for all PoC queries")
        CfnOutput(self, "SampleAthenaQuery",
                  value=f"SELECT * FROM {PREFIX}_database.customers LIMIT 10;",
                  description="Run as Alice (all columns) vs Bob (ssn column hidden)")


# Helper — avoids importing SecretValue at the top level
def cdk_secret_value(plain: str):
    from aws_cdk import SecretValue
    return SecretValue.unsafe_plain_text(plain)
