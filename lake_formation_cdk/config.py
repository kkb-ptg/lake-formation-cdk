"""
Centralised configuration for the Lake Formation CDK stack.
Bucket names use {account} and {region} placeholders resolved at synth time.
"""

PREFIX = "splashy"


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
