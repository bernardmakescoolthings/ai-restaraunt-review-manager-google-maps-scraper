#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import argparse
from src.monitor import Monitor

def setup_logging():
    """Create logs directory if it doesn't exist."""
    if not os.path.exists('logs'):
        os.makedirs('logs')

def main():
    # Create logs directory
    setup_logging()

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Google Maps Review Manager')
    parser.add_argument('--i', type=str, default='input/usernames.txt', 
                      help='target usernames file (default: input/usernames.txt)')
    parser.add_argument('--from-date', type=str, default='2020-01-01',
                      help='start date in format: YYYY-MM-DD (default: 2020-01-01)')
    
    args = parser.parse_args()

    # Check if usernames file exists
    if not os.path.exists(args.i):
        print(f"Error: Usernames file '{args.i}' not found.")
        print("Please create the file with one business username per line.")
        print("Each username should correspond to an entry in the businesses table.")
        sys.exit(1)

    # Initialize and run monitor
    try:
        monitor = Monitor(args.i, args.from_date)
        monitor.scrape_gm_reviews()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 