#!/bin/bash

# Make Python scripts executable
chmod +x view_database.py
chmod +x clear_database.py
chmod +x db_info.py

# Add tabulate to requirements.txt if not already present
if ! grep -q "tabulate" ../requirements.txt; then
    echo "tabulate>=0.8.0" >> ../requirements.txt
fi

echo "Database utility scripts are now ready to use!"
echo "Use './view_database.py --help' to view reviews"
echo "Use './clear_database.py --help' to clear reviews"
echo "Use './db_info.py' to view database schema and statistics"