#!/usr/bin/env python
# -*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
import sys
import argparse
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

def view_reviews(limit=10, business_url=None):
    """View reviews in the database."""
    conn = connect_to_db()
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            query = "SELECT id, id_review, caption, rating, username, timestamp, business_url FROM review"
            params = []
            
            if business_url:
                query += " WHERE business_url = %s"
                params.append(business_url)
                
            query += " ORDER BY timestamp DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            reviews = cursor.fetchall()
            
            if not reviews:
                print("No reviews found.")
                return
                
            # Get count of total reviews
            cursor.execute("SELECT COUNT(*) FROM review")
            total_count = cursor.fetchone()[0]
            
            # Get business URLs and their counts
            cursor.execute("""
                SELECT business_url, COUNT(*) 
                FROM review 
                GROUP BY business_url 
                ORDER BY COUNT(*) DESC
            """)
            business_counts = cursor.fetchall()
            
            # Print summary
            print(f"Total reviews in database: {total_count}")
            print("\nReviews per business:")
            for business, count in business_counts:
                print(f"  {business}: {count} reviews")
                
            # Convert reviews to a list of dictionaries for tabulate
            reviews_data = []
            for review in reviews:
                # Truncate caption for display
                caption = review['caption']
                if caption and len(caption) > 50:
                    caption = caption[:47] + "..."
                
                reviews_data.append({
                    'ID': review['id'],
                    'Rating': review['rating'],
                    'User': review['username'],
                    'Date': review['timestamp'].strftime('%Y-%m-%d'),
                    'Review': caption
                })
            
            print("\nMost recent reviews:")
            print(tabulate(reviews_data, headers="keys", tablefmt="grid"))
            
    except Exception as e:
        print(f"Error viewing reviews: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='View Google Maps reviews in database')
    parser.add_argument('--limit', type=int, default=10, help='Maximum number of reviews to display')
    parser.add_argument('--business', type=str, help='Filter by business URL')
    
    args = parser.parse_args()
    
    view_reviews(args.limit, args.business)