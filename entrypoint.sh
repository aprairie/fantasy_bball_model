#!/bin/bash
set -e

echo "Waiting for PostgreSQL to be ready..."
until PGPASSWORD="bball_password" psql -h "db" -U "bball_user" -d "basketball_stats" -c '\q'; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 1
done

echo "PostgreSQL is up - initializing database"
python database.py

# echo "Starting scraper"
# python scraper.py

echo "Starting model"
python model.py

echo "Scraping completed. Container will exit."
