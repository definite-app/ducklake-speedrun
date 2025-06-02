#!/usr/bin/env python3
"""Insert random event data into PostgreSQL table in a loop.

This script generates realistic application event data with various event types
and JSON details, inserting them into a PostgreSQL table repeatedly with configurable
loops, rows per loop, and delays.

Example usage with uv:
    # Install dependencies
    uv pip install psycopg2-binary python-dotenv

    # Run 100 loops, inserting 444 rows every 5 seconds
    uv run insert_event_data_loop.py --loops 100 --rows 444 --delay 5

    # Run continuously until interrupted (Ctrl+C)
    uv run insert_event_data_loop.py --continuous --rows 100 --delay 10

    # Run with custom connection string
    uv run insert_event_data_loop.py --conn "postgresql://user:pass@host:port/db" --loops 50

    # Truncate table before starting the loop
    uv run insert_event_data_loop.py --truncate --loops 10 --rows 1000 --delay 60

    # Run once (equivalent to original script)
    uv run insert_event_data_loop.py --loops 1 --rows 100 --delay 0
"""

import argparse
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any
import uuid

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Global flag for graceful shutdown
SHUTDOWN_REQUESTED = False

def signal_handler(signum, frame):
    """Handle interrupt signals gracefully."""
    global SHUTDOWN_REQUESTED
    print("\n\n🛑 Shutdown requested. Finishing current batch...")
    SHUTDOWN_REQUESTED = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Event type definitions with their relative weights (for uneven distribution)
EVENT_TYPES = {
    "user_login": {
        "weight": 35,  # Most common event
        "details_generator": lambda: {
            "user_id": f"user_{random.randint(1000, 9999)}",
            "ip_address": f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
            "user_agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) Firefox/121.0"
            ]),
            "session_id": str(uuid.uuid4()),
            "login_method": random.choice(["password", "oauth_google", "oauth_github", "sso"])
        }
    },
    "api_request": {
        "weight": 25,  # Second most common
        "details_generator": lambda: {
            "endpoint": random.choice([
                "/api/v1/users", "/api/v1/products", "/api/v1/orders", 
                "/api/v1/analytics", "/api/v1/reports"
            ]),
            "method": random.choice(["GET", "POST", "PUT", "DELETE"]),
            "status_code": random.choices(
                [200, 201, 204, 400, 401, 404, 500],
                weights=[50, 10, 5, 10, 5, 15, 5]
            )[0],
            "response_time_ms": random.randint(10, 2000),
            "request_id": str(uuid.uuid4()),
            "api_key": f"key_{random.randint(1000, 9999)}"
        }
    },
    "payment_processed": {
        "weight": 15,  # Less common
        "details_generator": lambda: {
            "transaction_id": str(uuid.uuid4()),
            "amount": round(random.uniform(10.0, 5000.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP", "JPY"]),
            "payment_method": random.choice(["credit_card", "paypal", "stripe", "bank_transfer"]),
            "status": random.choices(
                ["success", "failed", "pending"],
                weights=[80, 15, 5]
            )[0],
            "customer_id": f"cust_{random.randint(10000, 99999)}",
            "merchant_id": f"merch_{random.randint(100, 999)}"
        }
    },
    "error_occurred": {
        "weight": 10,  # Relatively rare
        "details_generator": lambda: {
            "error_code": random.choice(["ERR_001", "ERR_002", "ERR_003", "ERR_DB_001", "ERR_API_001"]),
            "error_message": random.choice([
                "Database connection timeout",
                "Invalid input parameters",
                "Service temporarily unavailable",
                "Rate limit exceeded",
                "Authentication failed"
            ]),
            "stack_trace": f"at line {random.randint(1, 500)} in {random.choice(['app.js', 'server.py', 'handler.go'])}",
            "severity": random.choice(["low", "medium", "high", "critical"]),
            "affected_users": random.randint(0, 1000),
            "service": random.choice(["auth-service", "payment-service", "notification-service", "data-service"])
        }
    },
    "feature_usage": {
        "weight": 15,  # Moderate frequency
        "details_generator": lambda: {
            "feature_name": random.choice([
                "advanced_search", "bulk_export", "real_time_analytics",
                "custom_dashboard", "api_integration", "team_collaboration"
            ]),
            "user_id": f"user_{random.randint(1000, 9999)}",
            "duration_seconds": random.randint(1, 3600),
            "clicks": random.randint(1, 50),
            "subscription_tier": random.choice(["free", "basic", "pro", "enterprise"]),
            "feature_version": f"v{random.randint(1, 3)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
        }
    }
}


def create_table_if_not_exists(conn):
    """Create the events table in the ducklake_src schema if it doesn't exist."""
    with conn.cursor() as cur:
        # Create schema if it doesn't exist
        cur.execute("CREATE SCHEMA IF NOT EXISTS ducklake_src;")
        
        # Create table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ducklake_src.events (
                event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                event_type VARCHAR(100) NOT NULL,
                event_timestamp TIMESTAMP NOT NULL,
                details JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create indexes for better query performance
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_type 
            ON ducklake_src.events(event_type);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_timestamp 
            ON ducklake_src.events(event_timestamp);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_details 
            ON ducklake_src.events USING GIN(details);
        """)
        
        conn.commit()
        print("✅ Table ducklake_src.events created/verified")


def truncate_table(conn):
    """Truncate the events table."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE ducklake_src.events;")
        conn.commit()
        print("🗑️  Table ducklake_src.events truncated")


def get_max_event_timestamp(conn) -> datetime:
    """Get the maximum event_timestamp from existing data."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(event_timestamp) 
            FROM ducklake_src.events
        """)
        result = cur.fetchone()
        if result and result[0]:
            return result[0]
        return None


def get_total_event_count(conn) -> int:
    """Get the total count of events in the table."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM ducklake_src.events")
        result = cur.fetchone()
        return result[0] if result else 0


def generate_event_data(num_rows: int, min_timestamp: datetime = None) -> List[Dict[str, Any]]:
    """Generate random event data with uneven distribution.
    
    Args:
        num_rows: Number of events to generate
        min_timestamp: If provided, all events will have timestamps after this time
    """
    events = []
    
    # Calculate weights for random selection
    event_names = list(EVENT_TYPES.keys())
    weights = [EVENT_TYPES[et]["weight"] for et in event_names]
    
    # Determine the base timestamp for generation
    base_timestamp = min_timestamp if min_timestamp else datetime.now() - timedelta(days=30)
    if min_timestamp:
        # Ensure we start at least 1 second after the last timestamp
        base_timestamp = min_timestamp + timedelta(seconds=1)
    
    # Generate events with timestamps spread over a shorter period for loop scenario
    # This ensures more recent-looking data
    for i in range(num_rows):
        # Select event type based on weights
        event_type = random.choices(event_names, weights=weights)[0]
        
        # For loop scenario, spread events over minutes/hours instead of days
        # This creates more realistic streaming data
        seconds_forward = i * random.uniform(0.1, 2.0)  # Spread events over time
        
        event_timestamp = base_timestamp + timedelta(seconds=seconds_forward)
        
        # Generate event details
        details = EVENT_TYPES[event_type]["details_generator"]()
        
        events.append({
            "event_type": event_type,
            "event_timestamp": event_timestamp,
            "details": details
        })
    
    # Sort events by timestamp for more realistic data
    events.sort(key=lambda x: x["event_timestamp"])
    
    return events


def insert_events(conn, events: List[Dict[str, Any]]) -> int:
    """Insert events into the database."""
    with conn.cursor() as cur:
        inserted = 0
        for event in events:
            try:
                cur.execute("""
                    INSERT INTO ducklake_src.events (event_type, event_timestamp, details)
                    VALUES (%s, %s, %s)
                """, (
                    event["event_type"],
                    event["event_timestamp"],
                    Json(event["details"])
                ))
                inserted += 1
            except Exception as e:
                print(f"❌ Error inserting event: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        return inserted


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def run_insert_loop(
    num_loops: int,
    rows_per_loop: int,
    delay_seconds: float,
    conn_str: str,
    truncate_first: bool = False,
    continuous: bool = False
):
    """Run the insert loop with specified parameters.
    
    Args:
        num_loops: Number of loops to run (ignored if continuous is True)
        rows_per_loop: Number of rows to insert per loop
        delay_seconds: Delay in seconds between loops
        conn_str: PostgreSQL connection string
        truncate_first: If True, truncate the table before starting
        continuous: If True, run continuously until interrupted
    """
    # Connect to database
    print(f"🔌 Connecting to PostgreSQL...")
    conn = psycopg2.connect(conn_str)
    
    try:
        # Create table if needed
        create_table_if_not_exists(conn)
        
        # Truncate table if requested
        if truncate_first:
            truncate_table(conn)
        
        # Get initial state
        initial_count = get_total_event_count(conn)
        max_timestamp = get_max_event_timestamp(conn)
        
        print(f"📊 Initial state: {initial_count:,} events in table")
        if max_timestamp:
            print(f"📅 Latest event timestamp: {max_timestamp}")
        
        # Loop configuration
        mode_str = "continuous mode" if continuous else f"{num_loops} loops"
        print(f"\n🔄 Starting {mode_str}")
        print(f"   - Rows per loop: {rows_per_loop:,}")
        print(f"   - Delay between loops: {delay_seconds}s")
        if not continuous:
            total_expected = num_loops * rows_per_loop
            estimated_time = num_loops * delay_seconds + (num_loops * 2)  # 2s estimate per insert
            print(f"   - Total rows to insert: {total_expected:,}")
            print(f"   - Estimated time: {format_duration(estimated_time)}")
        print()
        
        # Statistics tracking
        start_time = time.time()
        total_inserted = 0
        loop_count = 0
        errors = 0
        
        # Main loop
        while True:
            if SHUTDOWN_REQUESTED:
                print("\n🛑 Shutdown requested, stopping loop...")
                break
                
            if not continuous and loop_count >= num_loops:
                break
            
            loop_count += 1
            loop_start = time.time()
            
            # Progress indicator
            if continuous:
                print(f"🔄 Loop {loop_count} starting...", end='', flush=True)
            else:
                progress = (loop_count / num_loops) * 100
                print(f"🔄 Loop {loop_count}/{num_loops} ({progress:.1f}%) starting...", end='', flush=True)
            
            try:
                # Get current max timestamp for incremental data
                max_timestamp = get_max_event_timestamp(conn)
                
                # Generate and insert events
                events = generate_event_data(rows_per_loop, min_timestamp=max_timestamp)
                inserted = insert_events(conn, events)
                total_inserted += inserted
                
                # Calculate rates
                loop_duration = time.time() - loop_start
                rows_per_second = inserted / loop_duration if loop_duration > 0 else 0
                
                print(f" ✅ {inserted} rows in {loop_duration:.1f}s ({rows_per_second:.0f} rows/s)")
                
                # Show running statistics every 10 loops
                if loop_count % 10 == 0 or loop_count == num_loops:
                    elapsed = time.time() - start_time
                    overall_rate = total_inserted / elapsed if elapsed > 0 else 0
                    current_total = get_total_event_count(conn)
                    print(f"\n📈 Progress update:")
                    print(f"   - Loops completed: {loop_count}")
                    print(f"   - Rows inserted this session: {total_inserted:,}")
                    print(f"   - Total rows in table: {current_total:,}")
                    print(f"   - Overall rate: {overall_rate:.0f} rows/s")
                    print(f"   - Elapsed time: {format_duration(elapsed)}\n")
                
            except Exception as e:
                errors += 1
                print(f" ❌ Error: {e}")
                # Try to reconnect if connection was lost
                try:
                    conn.close()
                except:
                    pass
                try:
                    conn = psycopg2.connect(conn_str)
                    print("   🔌 Reconnected to database")
                except Exception as reconn_err:
                    print(f"   ❌ Failed to reconnect: {reconn_err}")
                    break
            
            # Delay between loops (if not the last loop)
            if delay_seconds > 0 and (continuous or loop_count < num_loops):
                if not SHUTDOWN_REQUESTED:
                    print(f"   💤 Waiting {delay_seconds}s before next loop...")
                    time.sleep(delay_seconds)
        
        # Final statistics
        total_duration = time.time() - start_time
        final_count = get_total_event_count(conn)
        
        print("\n" + "="*60)
        print("📊 Final Statistics:")
        print(f"   - Loops completed: {loop_count}")
        print(f"   - Rows inserted this session: {total_inserted:,}")
        print(f"   - Errors encountered: {errors}")
        print(f"   - Total rows in table: {final_count:,}")
        print(f"   - Total duration: {format_duration(total_duration)}")
        if total_inserted > 0:
            avg_rate = total_inserted / total_duration
            print(f"   - Average insert rate: {avg_rate:.0f} rows/s")
        
        # Show event distribution
        with conn.cursor() as cur:
            cur.execute("""
                SELECT event_type, COUNT(*) as count
                FROM ducklake_src.events
                GROUP BY event_type
                ORDER BY count DESC
            """)
            distribution = cur.fetchall()
            
            print("\n📈 Event distribution in table:")
            for event_type, count in distribution:
                percentage = (count / final_count) * 100 if final_count > 0 else 0
                print(f"   - {event_type}: {count:,} ({percentage:.1f}%)")
    
    finally:
        conn.close()
        print("\n✅ Database connection closed")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Insert random event data into PostgreSQL in a loop",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run 100 loops, inserting 444 rows every 5 seconds
  %(prog)s --loops 100 --rows 444 --delay 5
  
  # Run continuously until interrupted
  %(prog)s --continuous --rows 100 --delay 10
  
  # Run once (like the original script)
  %(prog)s --loops 1 --rows 1000 --delay 0
        """
    )
    
    # Loop configuration
    parser.add_argument(
        "--loops", "-l",
        type=int,
        default=1,
        help="Number of loops to run (default: 1)"
    )
    parser.add_argument(
        "--rows", "-r",
        type=int,
        default=100,
        help="Number of rows to insert per loop (default: 100)"
    )
    parser.add_argument(
        "--delay", "-d",
        type=float,
        default=5.0,
        help="Delay in seconds between loops (default: 5.0)"
    )
    parser.add_argument(
        "--continuous", "-c",
        action="store_true",
        help="Run continuously until interrupted (ignores --loops)"
    )
    
    # Database configuration
    parser.add_argument(
        "--conn",
        help="PostgreSQL connection string (defaults to POSTGRES_CONN_STR env var)"
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate the table before starting the loop"
    )
    
    args = parser.parse_args()
    
    # Get connection string
    conn_str = args.conn or os.getenv("POSTGRES_CONN_STR") or os.getenv("SUPABASE_PG_CONN")
    
    if not conn_str:
        print("❌ Error: No PostgreSQL connection string provided.")
        print("   Set POSTGRES_CONN_STR in .env or use --conn argument")
        return 1
    
    try:
        run_insert_loop(
            num_loops=args.loops,
            rows_per_loop=args.rows,
            delay_seconds=args.delay,
            conn_str=conn_str,
            truncate_first=args.truncate,
            continuous=args.continuous
        )
    except KeyboardInterrupt:
        print("\n\n🛑 Interrupted by user")
        return 0
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 