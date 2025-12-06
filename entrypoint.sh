#!/bin/bash
set -e

echo "Waiting for PostgreSQL to be ready..."
until PGPASSWORD="bball_password" psql -h "db" -U "bball_user" -d "basketball_stats" -c '\q'; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 1
done

echo "PostgreSQL is up - initializing database"
python database.py

# echo "figuring out main.py options"; python main.py

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

# echo "Scraping birthdays and saving to db"; python main.py scrape --birthdays
# echo "Scraping game stats and saving to db"; python main.py scrape --years 2026
# echo "Printing Player availabilities and saving to DB"; python main.py availability
# echo "Trial run of Head to Head Matchups"; python main.py h2h
# echo "Trial run of Trade Simulation"; python main.py trade --team1 "Alex" --team2 "David" -n 2 -t 0.0 --injured 
# echo "Trial run of Trade Simulation with Mandatory Players"; python main.py trade --team1 "Alex" --team2 "David" -n 3 -t 0.0 --injured --include "Kevin Durant" "Pascal Siakam" "Kristaps Porzingis"
# echo "Running generate z scores for players"; python main.py values
# echo "Exporting player stats to 'player_stats.csv'..."; python main.py export
# echo "Find best free agent pickups"; python main.py trade --team1 "Alex" --team2 "FreeAgents" -n 1
# echo "Find best free agent pickups"; python main.py trade --team1 "Alex" --team2 "FreeAgents" -n 2 --include "Joel Embiid"

echo "Try Exact Trade"; python main.py exact-trade \
  --team1 "Alex" --team2 "Simon" \
  --t1-gives "Trae Young" "Pascal Siakam" "Joel Embiid"\
  --t2-gives "Karl-Anthony Towns" "Bennedict Mathurin" "Quentin Grimes"

echo "Processing completed. Container will exit."
