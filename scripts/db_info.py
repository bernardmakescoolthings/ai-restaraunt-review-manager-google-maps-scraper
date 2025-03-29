#!/usr/bin/env python
# -*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
import sys
from tabulate import tabulate

# Load environment variables
load_dotenv()

# Default database configuration
DEFAULT_DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'googlemaps'),
    'user': os.getenv('DB_USER', 'reviewsuser'),
    'password': os.getenv('DB_PASSWORD', 'reviewspass')
}

def connect_to_db():
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DEFAULT_DB_CONFIG['host'],
            port=DEFAULT_DB_CONFIG['port'],
            database=DEFAULT_DB_CONFIG['database'],
            user=DEFAULT_DB_CONFIG['user'],
            password=DEFAULT_DB_CONFIG['password']
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        sys.exit(1)

def table_exists(cursor, table_name):
    """Check if a table exists in the database."""
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
    """, (table_name,))
    return cursor.fetchone()[0]

def print_table_schema(cursor, table_name):
    """Print detailed schema information for a table."""
    # Get column information
    cursor.execute("""
        SELECT column_name, data_type, character_maximum_length, 
               column_default, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
    """, (table_name,))
    
    columns = cursor.fetchall()
    
    # Format column information for display
    schema_data = []
    for col in columns:
        schema_data.append({
            'Column': col[0],
            'Type': col[1],
            'Max Length': col[2] if col[2] else 'N/A',
            'Default': col[3] if col[3] else 'N/A',
            'Nullable': col[4]
        })
    
    print(f"\nSchema for table '{table_name}':")
    print(tabulate(schema_data, headers="keys", tablefmt="grid"))

def print_table_statistics(cursor, table_name):
    """Print statistics for a table."""
    try:
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Get rating distribution
        cursor.execute(f"""
            SELECT rating, COUNT(*) as count
            FROM {table_name}
            GROUP BY rating
            ORDER BY rating;
        """)
        rating_dist = cursor.fetchall()
        
        # Get date range
        cursor.execute(f"""
            SELECT MIN(timestamp), MAX(timestamp)
            FROM {table_name};
        """)
        date_range = cursor.fetchone()
        
        # Get business statistics
        cursor.execute(f"""
            SELECT COUNT(DISTINCT business_url) as unique_businesses,
                   COUNT(DISTINCT username) as unique_reviewers
            FROM {table_name};
        """)
        business_stats = cursor.fetchone()
        
        # Format statistics for display
        stats_data = [
            ['Total Reviews', row_count],
            ['Unique Businesses', business_stats[0]],
            ['Unique Reviewers', business_stats[1]],
            ['Date Range', f"{date_range[0].strftime('%Y-%m-%d')} to {date_range[1].strftime('%Y-%m-%d')}" if date_range[0] else 'No data']
        ]
        
        print(f"\nStatistics for table '{table_name}':")
        print(tabulate(stats_data, tablefmt="grid"))
        
        if rating_dist:
            print("\nRating Distribution:")
            rating_data = [{'Rating': r[0], 'Count': r[1]} for r in rating_dist]
            print(tabulate(rating_data, headers="keys", tablefmt="grid"))
    except Exception as e:
        print(f"Error getting statistics for table '{table_name}': {e}")

def print_database_info():
    """Print comprehensive database information."""
    conn = connect_to_db()
    
    try:
        with conn.cursor() as cursor:
            # Get table information
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = cursor.fetchall()
            
            # Print database overview
            print("\nDatabase Overview:")
            print(f"Database Name: {DEFAULT_DB_CONFIG['database']}")
            
            if not tables:
                print("\nNo tables found in the database.")
                print("Run the monitor script first to create the review table.")
                return
            
            # Print table information
            print("\nTables in database:")
            table_data = [{'Table': t[0]} for t in tables]
            print(tabulate(table_data, headers="keys", tablefmt="grid"))
            
            # Print detailed information for each table
            for table in tables:
                print_table_schema(cursor, table[0])
                print_table_statistics(cursor, table[0])
                
    except Exception as e:
        print(f"Error getting database information: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    print_database_info() 