"""
Centralised configuration for the Lake Formation CDK stack.
All resource names are derived from PREFIX — change it here to rename everything.
Bucket names also embed {account} and {region}, resolved at synth time.
Note: S3 bucket names use hyphens — AWS does not allow underscores in bucket names.
"""

PREFIX = "splashy"

# ── IAM ───────────────────────────────────────────────────────────────────────
LF_SERVICE_ROLE_NAME  = f"{PREFIX}_lf_service_role"
GLUE_JOB_ROLE_NAME    = f"{PREFIX}_glue_job_role"
ANALYST_ALICE_NAME    = f"{PREFIX}_analyst_alice"
ANALYST_BOB_NAME      = f"{PREFIX}_analyst_bob"

# ── Glue ──────────────────────────────────────────────────────────────────────
GLUE_DATABASE_NAME    = f"{PREFIX}_database"
CRAWLER_NAME          = f"{PREFIX}_raw_customers_crawler"
GLUE_JOB_NAME         = f"{PREFIX}_etl"

# ── Lake Formation ────────────────────────────────────────────────────────────
ROW_FILTER_NAME       = f"{PREFIX}_bob_us_east_1_filter"

# ── Athena ────────────────────────────────────────────────────────────────────
ATHENA_WORKGROUP_NAME = f"{PREFIX}_workgroup"

# ── EventBridge / Step Functions ─────────────────────────────────────────────
EVENTBRIDGE_RULE_NAME = f"{PREFIX}_new_raw_data"
STATE_MACHINE_NAME    = f"{PREFIX}_ingest_pipeline"

# ── S3 bucket names (require account + region at synth time) ─────────────────
# Hyphens are intentional — AWS forbids underscores in S3 bucket names.

# Stores raw and curated customer data across raw/cleaned/curated S3 prefixes.
# Versioning is enabled so Glue ETL overwrites are recoverable.
def data_lake_bucket_name(account: str, region: str) -> str:
    return f"{PREFIX}-data-lake-{account}-{region}"


# Stores Athena query output files. The Athena workgroup enforces this location.
def athena_results_bucket_name(account: str, region: str) -> str:
    return f"{PREFIX}-athena-results-{account}-{region}"


# Holds the PySpark ETL script uploaded during cdk deploy via BucketDeployment.
def glue_scripts_bucket_name(account: str, region: str) -> str:
    return f"{PREFIX}-glue-scripts-{account}-{region}"
