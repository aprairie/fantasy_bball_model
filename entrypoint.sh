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

# echo "Starting birthday scraper"
# python birthday_scraper.py

# echo "Starting model"
# python model.py

# echo "Starting agent sim"
# python agent_sim.py

# echo "Dumping to csv"
# python dump_to_csv.py

# echo "Calculating Fantasy Value, must be done post-scraping"
# python calculate_fantasy_value.py

# echo "Dumping Player Values to csv"
# python dump_player_values_to_csv.py

# echo "Printing Player availabilities"
# python print_availability_main.py

echo "Trial run of player sampled games"
python trial_run_game_samples.py

echo "Processing completed. Container will exit."
