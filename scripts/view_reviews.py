#!/usr/bin/env python
# -*- coding: utf-8 -*-
import psycopg2
import os
from dotenv import load_dotenv
from tabulate import tabulate
import argparse
from datetime import datetime
import textwrap

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

def truncate_text(text, max_length=50):
    """Truncate text to max_length and add ellipsis if needed."""
    if not text:
        return ""
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + "..."

def format_timestamp(timestamp):
    """Format timestamp in a readable way."""
    if not timestamp:
        return ""
    return timestamp.strftime('%Y-%m-%d %H:%M')

def connect_to_db():
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DEFAULT_DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def get_reviews(limit=None, offset=None, business_url=None):
    """Get reviews from the database with optional filtering."""
    conn = connect_to_db()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            # Base query
            query = """
                SELECT id_review, caption, relative_date, retrieval_date, 
                       rating, username, n_review_user, url_user, 
                       timestamp, replies, business_url
                FROM review
                WHERE 1=1
            """
            params = []

            # Add business URL filter if specified
            if business_url:
                query += " AND business_url = %s"
                params.append(business_url)

            # Add ordering
            query += " ORDER BY retrieval_date DESC"

            # Add limit and offset if specified
            if limit:
                query += " LIMIT %s"
                params.append(limit)
            if offset:
                query += " OFFSET %s"
                params.append(offset)

            cursor.execute(query, params)
            reviews = cursor.fetchall()

            # Format the results
            headers = ['ID', 'Review', 'Date', 'Rating', 'User', 'Business']
            
            # Convert to list of dictionaries for tabulate
            formatted_reviews = []
            for review in reviews:
                # Format the review text with line breaks for readability
                review_text = f"{truncate_text(review[1])}\n{review[2]}"
                
                formatted_reviews.append({
                    'ID': review[0],
                    'Review': review_text,
                    'Date': format_timestamp(review[3]),
                    'Rating': f"{review[4]:.1f} ⭐" if review[4] else "N/A",
                    'User': f"{review[5]}\n({review[6]} reviews)",
                    'Business': truncate_text(review[10])
                })

            # Print the results
            if formatted_reviews:
                print("\n" + "="*80)
                print(f"Found {len(formatted_reviews)} reviews:")
                print("="*80 + "\n")
                
                # Use grid format with adjusted column widths
                print(tabulate(formatted_reviews, 
                             headers="keys", 
                             tablefmt="grid",
                             maxcolwidths=[None, 40, None, None, None, 30]))
                
                print("\n" + "="*80)
                print("Note: Reviews are truncated. Use --limit and --offset for pagination.")
                print("="*80)
            else:
                print("\nNo reviews found.")

    except Exception as e:
        print(f"Error fetching reviews: {e}")
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='View Google Maps reviews from database')
    parser.add_argument('--limit', type=int, help='Limit number of reviews to show')
    parser.add_argument('--offset', type=int, help='Offset for pagination')
    parser.add_argument('--business-url', type=str, help='Filter reviews by business URL')
    
    args = parser.parse_args()

    get_reviews(
        limit=args.limit,
        offset=args.offset,
        business_url=args.business_url
    )

if __name__ == '__main__':
    main() 