#!/usr/bin/env python3
"""Events Pipeline for Duck Lake

This script provides an incremental ETL pipeline specifically for the
ducklake_src.events table. It extracts new events from PostgreSQL,
stages them in DuckDB, and promotes them to an existing Duck Lake catalog.

The pipeline tracks state to ensure only new records are processed on each run.
"""

import argparse
import logging
import os
import traceback
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import duckdb
import dlt
from dlt.sources.sql_database import sql_database, sql_table
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def clean_postgres_connection_string(conn_str: str) -> str:
    """Remove search_path from connection string as it's not a valid DSN parameter.
    
    The search_path should be set after connection, not in the DSN.
    """
    # Parse the connection string
    parsed = urlparse(conn_str)
    
    # Parse query parameters
    query_params = parse_qs(parsed.query)
    
    # Remove search_path and csearch_path if present
    params_to_remove = ['search_path', 'csearch_path']
    for param in params_to_remove:
        if param in query_params:
            del query_params[param]
    
    # Reconstruct the query string
    new_query = urlencode({k: v[0] if isinstance(v, list) and len(v) == 1 else v 
                          for k, v in query_params.items()})
    
    # Reconstruct the full URL
    cleaned_url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment
    ))
    
    return cleaned_url


def _connect(database: str | None = None) -> duckdb.DuckDBPyConnection:
    """Return a duckdb connection with required extensions."""
    conn = duckdb.connect(database or ":memory:")
    conn.execute("FORCE INSTALL ducklake FROM core_nightly; LOAD ducklake;")
    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("SET pg_debug_show_queries=false;")
    return conn


def set_gcs_secret(
    conn: duckdb.DuckDBPyConnection,
    key_id: str,
    secret: str,
    persistent: bool = True,
) -> None:
    """Register a GCS HMAC key with DuckDB's secret manager."""
    tmpl = "CREATE OR REPLACE {persist} SECRET (TYPE gcs, KEY_ID '{key}', SECRET '{sec}');"
    conn.execute(
        tmpl.format(persist="PERSISTENT" if persistent else "", key=key_id, sec=secret)
    )


def attach_duck_lake(
    conn: duckdb.DuckDBPyConnection,
    catalog_conn: str,
    data_path: str,
    alias: str = "lake",
) -> None:
    """Attach an existing Duck Lake catalog."""
    # Get metadata schema from environment variable
    metadata_schema = os.getenv("POSTGRES_METADATA_SCHEMA", "ducklake")
    
    stmt = (
        "ATTACH 'ducklake:postgres:{conn}' AS {alias} (DATA_PATH '{data}', METADATA_SCHEMA '{schema}');"
    ).format(
        conn=catalog_conn.replace("'", "''"), 
        alias=alias, 
        data=data_path,
        schema=metadata_schema
    )
    conn.execute(stmt)
    # Remove USE statement as it may not work as expected with Duck Lake
    # conn.execute(f"USE {alias};")


def get_ducklake_schema_path(conn: duckdb.DuckDBPyConnection, alias: str = "lake") -> str:
    """Get the correct schema path for Duck Lake tables.
    
    Duck Lake might create tables in a specific schema pattern.
    This function helps identify the correct path.
    """
    try:
        # First, let's see all catalogs and schemas
        all_schemas = conn.execute("""
            SELECT DISTINCT catalog_name, schema_name 
            FROM information_schema.schemata 
            ORDER BY catalog_name, schema_name
        """).fetchall()
        
        # Look for the actual Duck Lake catalog (not the metadata catalog)
        # The alias should match a catalog that's not __ducklake_metadata_*
        duck_lake_schemas = conn.execute(f"""
            SELECT DISTINCT catalog_name, schema_name 
            FROM information_schema.schemata 
            WHERE catalog_name = '{alias}'
               AND schema_name NOT IN ('information_schema', 'pg_catalog')
            ORDER BY catalog_name, schema_name
        """).fetchall()
        
        print(f"DEBUG: Duck Lake catalog schemas for alias '{alias}':")
        for cat, schema in duck_lake_schemas:
            print(f"  - {cat}.{schema}")
        
        # Use the main schema in the Duck Lake catalog
        for cat, schema in duck_lake_schemas:
            if schema == 'main':
                path = f"{cat}.{schema}"
                print(f"DEBUG: Using Duck Lake main schema: {path}")
                return path
        
        # If no main schema, use the first available schema
        for cat, schema in duck_lake_schemas:
            path = f"{cat}.{schema}"
            print(f"DEBUG: Using first available Duck Lake schema: {path}")
            return path
        
        # If we still haven't found anything, just use lake.main as default
        default_path = f"{alias}.main"
        print(f"DEBUG: No Duck Lake schema found, using default: {default_path}")
        return default_path
        
    except Exception as e:
        print(f"Error getting Duck Lake schema path: {e}")
        logger.error(f"Error in get_ducklake_schema_path: {e}")
        logger.error(traceback.format_exc())
        # Return a reasonable default instead of failing
        return f"{alias}.main"


def extract_events_incremental(
    src_pg_conn: str,
    staging_db_file: str = "events_staging.duckdb",
    dataset_name: str = "staging",
) -> dict:
    """Extract events table incrementally using DLT.
    
    Uses the event_timestamp to load only new records since the last run.
    DLT automatically manages the state to track what has been loaded.
    """
    # Clean the connection string to remove invalid DSN parameters
    cleaned_conn = clean_postgres_connection_string(src_pg_conn)
    
    # First, let's check what's in the source table
    print("Checking source table...")
    try:
        import psycopg2
        from urllib.parse import urlparse
        
        # Parse connection string to get components
        parsed = urlparse(cleaned_conn)
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            user=parsed.username,
            password=parsed.password,
            database=parsed.path.lstrip('/')
        )
        cursor = conn.cursor()
        
        # Check if table exists and get row count
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'ducklake_src' 
            AND table_name = 'events'
        """)
        table_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            cursor.execute("SELECT COUNT(*) FROM ducklake_src.events")
            total_rows = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT MIN(event_timestamp), MAX(event_timestamp) 
                FROM ducklake_src.events
            """)
            min_ts, max_ts = cursor.fetchone()
            
            print(f"Source table has {total_rows} total rows")
            print(f"Event timestamp range: {min_ts} to {max_ts}")
        else:
            print("WARNING: Source table ducklake_src.events does not exist!")
            
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error checking source table: {e}")
        logger.error(f"Error checking source table: {e}")
        logger.error(traceback.format_exc())
    
    # Configure the SQL table with incremental loading on event_timestamp
    # Use timezone-aware datetime to avoid warnings
    events_table = sql_table(
        credentials=cleaned_conn,
        schema="ducklake_src",
        table="events",
        incremental=dlt.sources.incremental(
            "event_timestamp", 
            initial_value=datetime(2020, 1, 1, tzinfo=timezone.utc)
        )
    )
    
    # Create pipeline with state persistence
    pipeline = dlt.pipeline(
        pipeline_name="events_incremental",
        destination=dlt.destinations.duckdb(staging_db_file),
        dataset_name=dataset_name,
        dev_mode=False  # Ensure state is persisted
    )
    
    # Run the pipeline
    info = pipeline.run(events_table, write_disposition="append")
    
    # Get statistics
    load_info = {
        "rows_loaded": 0,
        "started_at": info.started_at,
        "finished_at": info.finished_at,
        "duration": (info.finished_at - info.started_at).total_seconds() if info.finished_at else 0
    }
    
    if info.load_packages:
        for package in info.load_packages:
            for job in package.jobs["completed_jobs"]:
                if hasattr(job, "row_count"):
                    load_info["rows_loaded"] += job.row_count
    
    return load_info


def promote_events_to_lake(
    staging_db_file: str,
    catalog_conn: str,
    data_path: str,
    dataset_name: str = "staging",
    alias: str = "lake",
    gcs_key_id: Optional[str] = None,
    gcs_secret: Optional[str] = None,
) -> int:
    """Promote staged events to Duck Lake using INSERT ... SELECT.
    
    This function assumes the events table already exists in Duck Lake
    and uses INSERT to add only the new records.
    """
    conn = _connect(staging_db_file)
    
    # Get the catalog name (usually the database filename without extension)
    catalog_name = os.path.splitext(os.path.basename(staging_db_file))[0]
    
    # Check what schemas exist in staging DB BEFORE attaching Duck Lake
    schemas = conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    print(f"Available schemas in staging DB: {[s[0] for s in schemas]}")
    
    # Check if the staging schema exists
    if dataset_name not in [s[0] for s in schemas]:
        print(f"Schema '{dataset_name}' not found in staging database. No events to promote.")
        return 0
    
    # Check if events table exists in staging
    tables = conn.execute(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{dataset_name}'
    """).fetchall()
    
    if 'events' not in [t[0] for t in tables]:
        print(f"Table 'events' not found in schema '{dataset_name}'. No events to promote.")
        return 0
    
    # Inspect the staging table structure
    print(f"\nInspecting staging table structure...")
    try:
        columns_info = conn.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_catalog = '{catalog_name}' 
            AND table_schema = '{dataset_name}' 
            AND table_name = 'events'
            ORDER BY ordinal_position
        """).fetchall()
        print(f"Staging table columns:")
        for col_name, col_type in columns_info:
            print(f"  - {col_name}: {col_type}")
    except Exception as e:
        print(f"Warning: Could not inspect table structure: {e}")
        logger.warning(f"Could not inspect table structure: {e}")
        logger.warning(traceback.format_exc())
    
    # Count new records in staging using full path
    staging_count = conn.execute(
        f"SELECT COUNT(*) FROM {catalog_name}.{dataset_name}.events"
    ).fetchone()[0]
    
    if staging_count == 0:
        print("No new events to promote")
        return 0
    
    print(f"Found {staging_count} new events in staging")
    
    # Sample a row to see actual data types
    try:
        sample_row = conn.execute(f"""
            SELECT * FROM {catalog_name}.{dataset_name}.events LIMIT 1
        """).fetchone()
        if sample_row:
            print(f"\nSample row data types:")
            for i, value in enumerate(sample_row):
                print(f"  - Column {i}: {type(value).__name__} = {str(value)[:50]}...")
    except Exception as e:
        print(f"Warning: Could not sample data: {e}")
        logger.warning(f"Could not sample data: {e}")
        logger.warning(traceback.format_exc())
    
    # Commit any pending staging operations before attaching Duck Lake
    try:
        conn.commit()
        print("Committed staging operations")
    except Exception as e:
        print(f"Warning: Could not commit staging operations: {e}")
    
    # NOW set GCS credentials and attach Duck Lake
    if gcs_key_id and gcs_secret:
        set_gcs_secret(conn, gcs_key_id, gcs_secret)
    
    print(f"\nAttaching Duck Lake catalog...")
    try:
        # Clean the catalog connection string before using it
        cleaned_catalog_conn = clean_postgres_connection_string(catalog_conn)
        attach_duck_lake(conn, cleaned_catalog_conn, data_path, alias)
        print(f"Successfully attached Duck Lake as '{alias}'")
    except Exception as e:
        print(f"Error attaching Duck Lake: {e}")
        logger.error(f"Error attaching Duck Lake: {e}")
        logger.error(traceback.format_exc())
        raise
    
    # Get the correct schema path for Duck Lake
    ducklake_path = get_ducklake_schema_path(conn, alias)
    print(f"Using Duck Lake path: {ducklake_path}")
    
    # Start a fresh transaction for Duck Lake operations
    try:
        conn.execute("COMMIT")  # Ensure no pending transaction
    except:
        pass  # Ignore if no transaction active
    
    # Check if events table exists in Duck Lake
    try:
        conn.execute("BEGIN")
        table_exists = conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'events' 
            AND table_catalog || '.' || table_schema = '{ducklake_path}'
        """).fetchone()[0] > 0
        conn.execute("COMMIT")
    except Exception as e:
        print(f"Error checking if events table exists in Duck Lake: {e}")
        logger.error(f"Error checking if events table exists: {e}")
        logger.error(traceback.format_exc())
        try:
            conn.execute("ROLLBACK")
        except:
            pass
        # Try a simpler approach
        try:
            print("Trying simpler table existence check...")
            conn.execute("BEGIN")
            # Just try to select from the table
            conn.execute(f"SELECT 1 FROM {ducklake_path}.events LIMIT 1")
            table_exists = True
            conn.execute("COMMIT")
        except:
            table_exists = False
            try:
                conn.execute("ROLLBACK")
            except:
                pass
    
    if not table_exists:
        # Create the table if it doesn't exist
        print("Creating events table in Duck Lake...")
        # Create table structure explicitly to avoid type issues
        try:
            print("Creating table structure in Duck Lake...")
            conn.execute("BEGIN")
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {ducklake_path}.events (
                    event_id VARCHAR,
                    event_type VARCHAR,
                    event_timestamp TIMESTAMP,
                    details JSON,
                    created_at TIMESTAMP
                )
            """)
            conn.execute("COMMIT")
            print("Table structure created successfully")
            
            # Now insert the data in a new transaction
            print("Inserting data into the new table...")
            conn.execute("BEGIN")
            conn.execute(f"""
                INSERT INTO {ducklake_path}.events
                SELECT 
                    event_id::VARCHAR,
                    event_type::VARCHAR,
                    event_timestamp::TIMESTAMP,
                    details::JSON,
                    created_at::TIMESTAMP
                FROM {catalog_name}.{dataset_name}.events
            """)
            conn.execute("COMMIT")
            print("Data inserted successfully")
        except Exception as e:
            print(f"Failed to create table with explicit structure: {e}")
            logger.error(f"Failed to create table with explicit structure: {e}")
            logger.error(traceback.format_exc())
            try:
                conn.execute("ROLLBACK")
            except:
                pass
            raise
    else:
        # Insert new records, avoiding duplicates based on event_id
        print("Inserting new events into Duck Lake...")
        try:
            # First, let's debug what's in the Duck Lake path
            print(f"\nDEBUG: Checking Duck Lake catalog structure...")
            try:
                debug_info = conn.execute(f"""
                    SELECT DISTINCT catalog_name, schema_name 
                    FROM information_schema.schemata 
                    WHERE catalog_name || '.' || schema_name = '{ducklake_path}'
                """).fetchall()
                print(f"Duck Lake path components: {debug_info}")
            except Exception as e:
                print(f"Could not debug Duck Lake path: {e}")
            
            conn.execute("BEGIN")
            conn.execute(f"""
                INSERT INTO {ducklake_path}.events 
                SELECT 
                    s.event_id::VARCHAR,
                    s.event_type::VARCHAR,
                    s.event_timestamp::TIMESTAMP,
                    s.details::JSON,
                    s.created_at::TIMESTAMP
                FROM {catalog_name}.{dataset_name}.events s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {ducklake_path}.events l 
                    WHERE l.event_id = s.event_id
                )
            """)
            conn.execute("COMMIT")
        except Exception as e:
            print(f"Error inserting events into Duck Lake: {e}")
            logger.error(f"Error inserting events: {e}")
            logger.error(traceback.format_exc())
            try:
                conn.execute("ROLLBACK")
            except:
                pass
            raise
    
    # Get count of records actually inserted
    try:
        conn.execute("BEGIN")
        inserted_count = conn.execute(
            f"SELECT COUNT(*) FROM {ducklake_path}.events"
        ).fetchone()[0]
        conn.execute("COMMIT")
    except Exception as e:
        print(f"Error getting count from Duck Lake: {e}")
        logger.error(f"Error getting count: {e}")
        logger.error(traceback.format_exc())
        try:
            conn.execute("ROLLBACK")
        except:
            pass
        # Don't raise here, just set a default
        inserted_count = -1
    
    # Clear staging table for next run
    try:
        conn.execute("BEGIN")
        conn.execute(f"DELETE FROM {catalog_name}.{dataset_name}.events")
        conn.execute("COMMIT")
        print("Cleared staging table for next run")
    except Exception as e:
        print(f"Warning: Could not clear staging table: {e}")
        try:
            conn.execute("ROLLBACK")
        except:
            pass
    
    return staging_count


def run_events_pipeline(
    src_pg_conn: str,
    catalog_conn: str,
    data_path: str,
    gcs_key_id: Optional[str] = None,
    gcs_secret: Optional[str] = None,
    staging_db: str = "events_staging.duckdb",
) -> None:
    """Run the complete events pipeline: extract → stage → promote."""
    
    print("🚀 Starting Events Pipeline")
    print("=" * 60)
    
    # Step 1: Extract events incrementally
    print("\n📥 Step 1: Extracting new events from PostgreSQL...")
    load_info = extract_events_incremental(
        src_pg_conn=src_pg_conn,
        staging_db_file=staging_db,
        dataset_name="staging"  # Explicitly set to avoid confusion
    )
    
    print(f"✅ Extraction complete:")
    print(f"   - Rows loaded: {load_info['rows_loaded']}")
    print(f"   - Duration: {load_info['duration']:.2f} seconds")
    
    # Step 2: Promote to Duck Lake
    print("\n📤 Step 2: Promoting events to Duck Lake...")
    promoted_count = promote_events_to_lake(
        staging_db_file=staging_db,
        catalog_conn=catalog_conn,
        data_path=data_path,
        dataset_name="staging",  # Match the extraction dataset name
        gcs_key_id=gcs_key_id,
        gcs_secret=gcs_secret
    )
    
    print(f"✅ Promotion complete: {promoted_count} events promoted")
    
    # Step 3: Verify results
    print("\n📊 Step 3: Verifying results...")
    conn = _connect()
    if gcs_key_id and gcs_secret:
        set_gcs_secret(conn, gcs_key_id, gcs_secret)
    # Clean the catalog connection string before using it
    cleaned_catalog_conn = clean_postgres_connection_string(catalog_conn)
    attach_duck_lake(conn, cleaned_catalog_conn, data_path)
    
    # Get the correct Duck Lake path
    ducklake_path = get_ducklake_schema_path(conn)
    
    # Get total count and recent events
    total_count = conn.execute(f"SELECT COUNT(*) FROM {ducklake_path}.events").fetchone()[0]
    recent_events = conn.execute(f"""
        SELECT event_type, COUNT(*) as count
        FROM {ducklake_path}.events
        WHERE event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
        GROUP BY event_type
        ORDER BY count DESC
    """).fetchall()
    
    print(f"\n✅ Total events in Duck Lake: {total_count}")
    if recent_events:
        print("\n📈 Recent events (last hour):")
        for event_type, count in recent_events:
            print(f"   - {event_type}: {count}")
    
    print("\n✨ Pipeline completed successfully!")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Incremental ETL pipeline for ducklake_src.events table"
    )
    
    # Connection arguments
    parser.add_argument(
        "--source",
        default=os.getenv("POSTGRES_CONN_STR") or os.getenv("SUPABASE_PG_CONN"),
        help="PostgreSQL connection string for source data (defaults to env var)"
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("POSTGRES_CONN_STR") or os.getenv("SUPABASE_PG_CONN"),
        help="PostgreSQL connection string for Duck Lake catalog (defaults to env var)"
    )
    parser.add_argument(
        "--data-path",
        required=True,
        help="Parquet storage path (e.g., gs://bucket/ducklake/)"
    )
    
    # GCS credentials
    parser.add_argument(
        "--gcs-key-id",
        default=os.getenv("GCS_KEY_ID"),
        help="GCS HMAC key ID (defaults to env var)"
    )
    parser.add_argument(
        "--gcs-secret",
        default=os.getenv("GCS_SECRET"),
        help="GCS HMAC secret (defaults to env var)"
    )
    
    # Staging options
    parser.add_argument(
        "--staging-db",
        default="events_staging.duckdb",
        help="Staging database file (default: events_staging.duckdb)"
    )
    
    # Actions
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run the complete pipeline"
    )
    parser.add_argument(
        "--extract-only",
        action="store_true",
        help="Only run the extraction step"
    )
    parser.add_argument(
        "--promote-only",
        action="store_true",
        help="Only run the promotion step (assumes extraction already done)"
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Reset the incremental loading state (will reload all data)"
    )
    
    args = parser.parse_args()
    
    # Validate required arguments
    if not args.source:
        print("Error: No source connection string provided. Set POSTGRES_CONN_STR in .env or use --source")
        return 1
    
    if not args.catalog:
        print("Error: No catalog connection string provided. Set POSTGRES_CONN_STR in .env or use --catalog")
        return 1
    
    # Reset state if requested
    if args.reset_state:
        print("Resetting incremental loading state...")
        import shutil
        state_path = os.path.expanduser("~/.dlt/pipelines/events_incremental")
        if os.path.exists(state_path):
            shutil.rmtree(state_path)
            print(f"✅ State reset. Next run will load all data.")
        else:
            print("No state found to reset.")
        
        # Also delete staging database if it exists
        if os.path.exists(args.staging_db):
            os.remove(args.staging_db)
            print(f"✅ Removed staging database: {args.staging_db}")
    
    try:
        if args.extract_only:
            print("Running extraction only...")
            load_info = extract_events_incremental(
                src_pg_conn=args.source,
                staging_db_file=args.staging_db
            )
            print(f"✅ Extracted {load_info['rows_loaded']} rows")
            
        elif args.promote_only:
            print("Running promotion only...")
            promoted = promote_events_to_lake(
                staging_db_file=args.staging_db,
                catalog_conn=args.catalog,
                data_path=args.data_path,
                gcs_key_id=args.gcs_key_id,
                gcs_secret=args.gcs_secret
            )
            print(f"✅ Promoted {promoted} events")
            
        else:  # Default: run complete pipeline
            run_events_pipeline(
                src_pg_conn=args.source,
                catalog_conn=args.catalog,
                data_path=args.data_path,
                gcs_key_id=args.gcs_key_id,
                gcs_secret=args.gcs_secret,
                staging_db=args.staging_db
            )
            
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 