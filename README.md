# AWS Lake Formation + Glue Integration PoC — CDK Python

CDK Python implementation of the [DataCamp Lake Formation + Glue tutorial](https://www.datacamp.com/tutorial/aws-lake-formation-and-glue-integration).

## What gets deployed

| Resource | Details |
|---|---|
| **S3 — data lake bucket** | `raw/customers/`, `cleaned/`, `curated/customers/` zones |
| **S3 — Athena results** | Query output storage |
| **S3 — Glue scripts** | Hosts the PySpark ETL script |
| **IAM — GlueJobRole** | Used by crawler + ETL job, registered as LF admin |
| **IAM — analyst-alice** | Full SELECT on all tables |
| **IAM — analyst-bob** | SELECT without `ssn` column; `us-east-1` rows only |
| **Lake Formation admin** | Glue role registered, IAMAllowedPrincipals disabled |
| **LF data location** | S3 bucket registered with Lake Formation |
| **Glue Database** | `poc_database` |
| **Glue Crawler** | Scans `raw/customers/` → creates `customers` table |
| **Glue ETL Job** | CSV → Parquet, writes `curated_customers`, updates catalog |
| **LF-Tag** | `sensitivity = public \| internal \| confidential` |
| **LF Permissions** | Column-level (Bob, no SSN) + row-level (Bob, us-east-1) |
| **Athena Workgroup** | `lake-formation-poc` |

## Prerequisites

- Python 3.9+
- Node.js 18+ (CDK CLI)
- AWS CLI configured (`aws configure`)
- AWS CDK CLI: `npm install -g aws-cdk`

## Deploy

```bash
# 1. Clone / copy this project
cd lake-formation-cdk

# 2. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Bootstrap CDK (once per account/region)
cdk bootstrap aws://YOUR_ACCOUNT_ID/us-east-1

# 5. Deploy
cdk deploy --context account=YOUR_ACCOUNT_ID --context region=us-east-1
```

## Post-deploy steps

After `cdk deploy` completes:

### Step 1 — Run the Glue Crawler
```bash
aws glue start-crawler --name raw-customers-crawler
# Wait for it to reach READY state (~1-2 min)
aws glue get-crawler --name raw-customers-crawler --query 'Crawler.State'
```

### Step 2 — Run the Glue ETL Job
```bash
aws glue start-job-run --job-name LF-GlueStudio-ETL
# Monitor status
aws glue get-job-runs --job-name LF-GlueStudio-ETL \
  --query 'JobRuns[0].{Status:JobRunState,Duration:ExecutionTime}'
```

### Step 3 — Query with Athena (as alice — full access)
```sql
-- Run in Athena workgroup: lake-formation-poc
SELECT * FROM poc_database.customers LIMIT 10;
-- All columns including ssn should be visible
```

### Step 4 — Query with Athena (as bob — restricted)
```sql
-- Switch console role/user to analyst-bob
SELECT * FROM poc_database.customers LIMIT 10;
-- ssn column will NOT appear in results
-- Only us-east-1 rows visible on curated_customers
```

### Step 5 — Verify row-level filter on Bob
```sql
SELECT region, COUNT(*) as cnt
FROM poc_database.curated_customers
GROUP BY region;
-- Bob sees only region = 'us-east-1' rows
```

## Project structure

```
lake-formation-cdk/
├── app.py                          # CDK app entrypoint
├── cdk.json
├── requirements.txt
├── assets/
│   ├── data/
│   │   └── customers.csv           # Sample data (uploaded to raw/ zone)
│   └── scripts/
│       └── etl_csv_to_parquet.py   # PySpark Glue ETL job
└── lake_formation_cdk/
    └── lake_formation_stack.py     # Main CDK stack
```

## Cleanup

```bash
cdk destroy --context account=YOUR_ACCOUNT_ID --context region=us-east-1
```

> **Note**: Buckets are set to `DESTROY` + `auto_delete_objects=True` for PoC convenience.  
> Change these to `RETAIN` for production use.
