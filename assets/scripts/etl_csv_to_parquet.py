import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_date, current_timestamp

# ── Job arguments ──────────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_DATABASE",
        "SOURCE_TABLE",
        "TARGET_PATH",
        "TARGET_DATABASE",
        "TARGET_TABLE",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── 1. Read from Glue Data Catalog (raw CSV) ───────────────────────────────────
print(f"Reading table: {args['SOURCE_DATABASE']}.{args['SOURCE_TABLE']}")
raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args["SOURCE_DATABASE"],
    table_name=args["SOURCE_TABLE"],
    transformation_ctx="raw_dyf",
)

print(f"Raw record count: {raw_dyf.count()}")
raw_dyf.printSchema()

# ── 2. Apply type mappings ─────────────────────────────────────────────────────
mapped_dyf = ApplyMapping.apply(
    frame=raw_dyf,
    mappings=[
        ("customer_id",  "string", "customer_id",  "int"),
        ("first_name",   "string", "first_name",   "string"),
        ("last_name",    "string", "last_name",    "string"),
        ("email",        "string", "email",        "string"),
        ("ssn",          "string", "ssn",          "string"),   # will be restricted via LF column security
        ("region",       "string", "region",       "string"),
        ("signup_date",  "string", "signup_date",  "string"),
        ("annual_spend", "string", "annual_spend", "double"),
    ],
    transformation_ctx="mapped_dyf",
)

# ── 3. Additional Spark transformations ────────────────────────────────────────
df = mapped_dyf.toDF()

# Parse signup_date to proper date type
df = df.withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))

# Add an ingestion timestamp
df = df.withColumn("ingested_at", current_timestamp())

# Drop nulls in key columns
df = df.dropna(subset=["customer_id", "email"])

print(f"Cleaned record count: {df.count()}")

cleaned_dyf = DynamicFrame.fromDF(df, glueContext, "cleaned_dyf")

# ── 4. Resolve any remaining choice types ─────────────────────────────────────
resolved_dyf = ResolveChoice.apply(
    frame=cleaned_dyf,
    choice="make_cols",
    transformation_ctx="resolved_dyf",
)

# ── 5. Write Parquet to curated S3 zone + update Glue catalog ─────────────────
print(f"Writing Parquet to: {args['TARGET_PATH']}")
sink = glueContext.getSink(
    path=args["TARGET_PATH"],
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="sink",
)
sink.setCatalogInfo(
    catalogDatabase=args["TARGET_DATABASE"],
    catalogTableName=args["TARGET_TABLE"],
)
sink.setFormat("glueparquet")
sink.writeFrame(resolved_dyf)

print("ETL job completed successfully.")
job.commit()
