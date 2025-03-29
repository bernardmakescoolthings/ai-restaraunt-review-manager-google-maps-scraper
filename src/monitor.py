#!/usr/bin/env python
# -*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
from .googlemaps import GoogleMapsScraper
from datetime import datetime, timedelta
import argparse
import logging
import sys

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

class Monitor:

    def __init__(self, url_file, from_date):
        # load urls file
        with open(url_file, 'r') as furl:
            self.urls = [u[:-1] for u in furl]

        # Connect to PostgreSQL
        self.conn = self._connect_to_db()
        
        # Create tables if they don't exist
        self._create_tables()

        # min date review to scrape
        self.min_date_review = datetime.strptime(from_date, '%Y-%m-%d')

        # logging
        self.logger = self.__get_logger()

    def _connect_to_db(self):
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
            self.logger.error(f"Error connecting to PostgreSQL: {e}")
            sys.exit(1)
            
    def _create_tables(self):
        """Create necessary tables if they don't exist."""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS review (
                    id SERIAL PRIMARY KEY,
                    id_review TEXT UNIQUE,
                    caption TEXT,
                    relative_date TEXT,
                    retrieval_date TIMESTAMP,
                    rating FLOAT,
                    username TEXT,
                    n_review_user INTEGER,
                    url_user TEXT,
                    timestamp TIMESTAMP,
                    replies TEXT,
                    business_url TEXT
                )
            """)

    def scrape_gm_reviews(self):
        # init scraper and incremental add reviews
        with GoogleMapsScraper() as scraper:
            for url in self.urls:
                try:
                    # Use sort_by with index 1 for newest reviews
                    error = scraper.sort_by(url, 1)  # 1 represents 'newest' in the ind dictionary
                    if error == 0:
                        stop = False
                        offset = 0
                        n_new_reviews = 0
                        while not stop:
                            rlist = scraper.get_reviews(offset)
                            if len(rlist) == 0:
                                break
                            for r in rlist:
                                # calculate review date and compare to input min_date_review
                                r['timestamp'] = self.__parse_relative_date(r['relative_date'])
                                # Add business URL to the review
                                r['business_url'] = url
                                stop = self.__stop(r)
                                if not stop:
                                    self._insert_review(r)
                                    n_new_reviews += 1
                                else:
                                    break
                            offset += len(rlist)

                        # log total number
                        self.logger.info('{} : {} new reviews'.format(url, n_new_reviews))
                    else:
                        self.logger.warning('Sorting reviews failed for {}'.format(url))

                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

                    self.logger.error('{}: {}, {}, {}'.format(url, exc_type, fname, exc_tb.tb_lineno))

    def _insert_review(self, review):
        """Insert a review into the PostgreSQL database."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO review (
                        id_review, caption, relative_date, retrieval_date, 
                        rating, username, n_review_user, url_user, timestamp,
                        replies, business_url
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id_review) DO NOTHING
                """, (
                    review.get('id_review'),
                    review.get('caption'),
                    review.get('relative_date'),
                    review.get('retrieval_date'),
                    review.get('rating'),
                    review.get('username'),
                    review.get('n_review_user', 0),
                    review.get('url_user'),
                    review.get('timestamp'),
                    None,  # replies initially set to null
                    review.get('business_url')
                ))
        except Exception as e:
            self.logger.error(f"Error inserting review: {e}")

    def __parse_relative_date(self, string_date):
        curr_date = datetime.now()
        split_date = string_date.split(' ')

        n = split_date[0]
        delta = split_date[1]

        if delta == 'year':
            return curr_date - timedelta(days=365)
        elif delta == 'years':
            return curr_date - timedelta(days=365 * int(n))
        elif delta == 'month':
            return curr_date - timedelta(days=30)
        elif delta == 'months':
            return curr_date - timedelta(days=30 * int(n))
        elif delta == 'week':
            return curr_date - timedelta(weeks=1)
        elif delta == 'weeks':
            return curr_date - timedelta(weeks=int(n))
        elif delta == 'day':
            return curr_date - timedelta(days=1)
        elif delta == 'days':
            return curr_date - timedelta(days=int(n))
        elif delta == 'hour':
            return curr_date - timedelta(hours=1)
        elif delta == 'hours':
            return curr_date - timedelta(hours=int(n))
        elif delta == 'minute':
            return curr_date - timedelta(minutes=1)
        elif delta == 'minutes':
            return curr_date - timedelta(minutes=int(n))
        elif delta == 'moments':
            return curr_date - timedelta(seconds=1)


    def __stop(self, r):
        """Check if we should stop scraping based on review date or if it already exists."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM review WHERE id_review = %s", (r['id_review'],))
                is_old_review = cursor.fetchone()
                
            if is_old_review is None and r['timestamp'] >= self.min_date_review:
                return False
            else:
                return True
        except Exception as e:
            self.logger.error(f"Error checking if review exists: {e}")
            return True

    def __get_logger(self):
        # create logger
        logger = logging.getLogger('monitor')
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        fh = logging.FileHandler('logs/monitor.log')
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        # add formatter to ch
        fh.setFormatter(formatter)
        # add ch to logger
        logger.addHandler(fh)

        return logger

def main():
    parser = argparse.ArgumentParser(description='Monitor Google Maps places')
    parser.add_argument('--i', type=str, default='input/urls.txt', help='target URLs file')
    parser.add_argument('--from-date', type=str, default='2022-01-01', help='start date in format: YYYY-MM-DD')

    args = parser.parse_args()

    monitor = Monitor(args.i, args.from_date)

    try:
        monitor.scrape_gm_reviews()
    except Exception as e:
        monitor.logger.error('Not handled error: {}'.format(e))

if __name__ == '__main__':
    main()
