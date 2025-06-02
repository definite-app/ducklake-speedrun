# Duck Lake Speedrun

uv run events_pipeline.py --run --data-path gs://ducklake-test/speedrun/events

A Python ETL utility for creating and managing Duck Lake catalogs backed by PostgreSQL with Parquet data stored on Google Cloud Storage (GCS).

## Features

- Create and manage Duck Lake catalogs
- ETL pipeline using dlt (Data Load Tool)
- Stage data from PostgreSQL to DuckDB
- Promote staged tables to Duck Lake
- Generate text embeddings using OpenAI API
- Support for GCS bucket authentication via HMAC keys
- Native JSON column support for complex data types

## Events Pipeline

The project includes a specialized incremental ETL pipeline for event data with native JSON support.

### Inserting Test Event Data

```bash
# Insert 100 random events (default)
uv run insert_event_data.py

# Insert 500 events
uv run insert_event_data.py --rows 500

# Truncate table and insert fresh data
uv run insert_event_data.py --truncate --rows 1000

# Events are automatically generated with realistic distributions
```

### Running the Events Pipeline

```bash
# Run complete incremental ETL pipeline
uv run events_pipeline.py --run --data-path gs://ducklake-test/speedrun

# Extract only (to staging)
uv run events_pipeline.py --extract-only

# Promote only (from staging to Duck Lake)
uv run events_pipeline.py --promote-only --data-path gs://ducklake-test/speedrun
```

### Querying JSON Data

Duck Lake now supports native JSON columns. The events table includes a `details` JSON column that can be queried using DuckDB's JSON functions:

```python
# Example: Extract specific fields from JSON
SELECT 
    event_timestamp,
    json_extract_string(details, '$.user_id') as user_id,
    json_extract_string(details, '$.login_method') as login_method
FROM events
WHERE event_type = 'user_login'

# Example: Filter by JSON field values
SELECT * FROM events
WHERE json_extract_string(details, '$.status') = 'failed'

# Example: Aggregate by JSON fields
SELECT 
    json_extract_string(details, '$.endpoint') as endpoint,
    COUNT(*) as request_count
FROM events
WHERE event_type = 'api_request'
GROUP BY endpoint
```

See `query_events_json.py` for more comprehensive examples of JSON querying.

## Prerequisites

- Python ≥3.10
- PostgreSQL database (e.g., Supabase)
- Google Cloud Storage bucket with HMAC credentials
- OpenAI API key (for embeddings)

## Installation

This project uses [uv](https://docs.astral.sh/uv) for dependency management.

1. Clone the repository:
```bash
git clone <your-repo-url>
cd ducklake-speedrun
```

2. Install dependencies with uv:
```bash
uv sync
```

This will create a virtual environment and install all required packages.

## Configuration

Create a `.env` file in the project root with your credentials:

```env
# PostgreSQL connection string
POSTGRES_CONN_STR=host=your-host.com port=5432 dbname=your-db user=your-user password=your-password sslmode=require

# GCS HMAC credentials
GCS_KEY_ID=GOOG1EXAMPLEACCESSIDSTRING
GCS_SECRET=your-base64-encoded-secret

# OpenAI API key
OPENAI_API_KEY=sk-your-openai-api-key
```

### Getting GCS HMAC Keys

1. Create a service account in your Google Cloud project:
```bash
gcloud iam service-accounts create ducklake-service \
    --display-name="Duck Lake Service Account"
```

2. Grant necessary permissions:
```bash
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:ducklake-service@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"
```

3. Create HMAC keys:
```bash
gcloud storage hmac create ducklake-service@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

## Architecture

1. **dlt Pipeline**: Extracts data from PostgreSQL source into a local DuckDB staging file
2. **Duck Lake Promotion**: Copies staged tables into the Duck Lake catalog (PostgreSQL metadata + GCS Parquet files)
3. **Embeddings**: Uses OpenAI API to generate vector embeddings for text columns

## Troubleshooting

### HMAC Key Issues
- Ensure your service account has the necessary GCS permissions
- Keys can take up to 60 seconds to become active after creation
- If you lose the secret, you must create new HMAC keys