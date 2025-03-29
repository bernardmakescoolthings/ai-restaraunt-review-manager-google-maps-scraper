#!/usr/bin/env python
# -*- coding: utf-8 -*-
import psycopg2
import os
from dotenv import load_dotenv
import sys
import argparse

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

def clear_reviews(business_url=None):
    """Clear reviews from database, optionally filtered by business URL."""
    conn = connect_to_db()
    
    try:
        with conn.cursor() as cursor:
            # Check if table exists
            if not table_exists(cursor, 'review'):
                print("No reviews table found. The database is already empty.")
                return
            
            # First get count of reviews to be deleted
            query = "SELECT COUNT(*) FROM review"
            params = []
            
            if business_url:
                query += " WHERE business_url = %s"
                params.append(business_url)
                
            cursor.execute(query, params)
            count = cursor.fetchone()[0]
            
            # Ask for confirmation before deleting
            if count == 0:
                print("No reviews to delete.")
                return
                
            if business_url:
                confirm = input(f"Are you sure you want to delete {count} reviews for {business_url}? (y/n): ")
            else:
                confirm = input(f"Are you sure you want to delete ALL {count} reviews from the database? (y/n): ")
                
            if confirm.lower() != 'y':
                print("Operation cancelled.")
                return
                
            # Delete reviews
            query = "DELETE FROM review"
            if business_url:
                query += " WHERE business_url = %s"
                cursor.execute(query, [business_url])
                print(f"{count} reviews deleted for {business_url}.")
            else:
                cursor.execute(query)
                print(f"All {count} reviews deleted from database.")
                
    except Exception as e:
        print(f"Error clearing reviews: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Clear Google Maps reviews from database')
    parser.add_argument('--business', type=str, help='Clear only reviews for a specific business URL')
    
    args = parser.parse_args()
    
    clear_reviews(args.business) 