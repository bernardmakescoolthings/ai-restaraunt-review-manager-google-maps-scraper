# Database Management Scripts

This directory contains scripts for managing and viewing the Google Maps reviews database.

## Scripts Overview

### 1. view_reviews.py
View reviews from the database in a formatted table.

**Usage:**
```bash
python view_reviews.py [options]
```

**Parameters:**
- `--limit <number>`: Limit the number of reviews to show
- `--offset <number>`: Offset for pagination
- `--business-url <url>`: Filter reviews by business URL

**Example:**
```bash
# View all reviews
python view_reviews.py

# View 10 reviews for a specific business
python view_reviews.py --limit 10 --business-url "https://www.google.com/maps/place/your-business"

# View reviews with pagination
python view_reviews.py --limit 10 --offset 20
```

### 2. clear_database.py
Clear all reviews from the database or for a specific business.

**Usage:**
```bash
python clear_database.py [options]
```

**Parameters:**
- `--business-url <url>`: Clear reviews for a specific business only (optional)

**Example:**
```bash
# Clear all reviews
python clear_database.py

# Clear reviews for a specific business
python clear_database.py --business-url "https://www.google.com/maps/place/your-business"
```

### 3. db_info.py
Display database information including table structure and statistics.

**Usage:**
```bash
python db_info.py
```

**No parameters required.**

### 4. monitor.py
Monitor and scrape Google Maps reviews for specified businesses.

**Usage:**
```bash
python monitor.py [options]
```

**Parameters:**
- `--i <file>`: Input file containing business URLs (default: input/urls.txt)
- `--from-date <YYYY-MM-DD>`: Start date for scraping reviews (default: 2022-01-01)

**Example:**
```bash
# Monitor reviews using default settings
python monitor.py

# Monitor reviews with custom input file and date
python monitor.py --i custom_urls.txt --from-date 2023-01-01
```

## Environment Variables

All scripts use the following environment variables from `.env` file:
- `DB_HOST`: Database host (default: localhost)
- `DB_PORT`: Database port (default: 5432)
- `DB_NAME`: Database name (default: googlemaps)
- `DB_USER`: Database user (default: reviewsuser)
- `DB_PASSWORD`: Database password (default: reviewspass)

## Notes

1. Make sure you're in the virtual environment before running any scripts:
```bash
source ../venv/bin/activate  # From the scripts directory
```

2. Ensure the `.env` file is properly configured with database credentials.

3. The scripts should be run from the `scripts` directory.

4. All scripts use the same database configuration and connection handling.

5. If you're not in the virtual environment, you can also run scripts using the full path:
```bash
../venv/bin/python script_name.py [options]
``` 