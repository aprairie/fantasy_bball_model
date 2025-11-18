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


# echo "Trial run of player sampled games"
# python trial_run_game_samples.py

# echo "Trial run of teams and players"
# python trial_run_teams_and_players.py

# echo "Scraping birthdays and saving to db"; python main.py scrape --birthdays
echo "Scraping game stats and saving to db"; python main.py scrape --years 2018 2019
# echo "Printing Player availabilities and saving to DB"; python main.py availability
# echo "Trial run of Head to Head Matchups"; python main.py h2h
# echo "Trial run of Trade Simulation"; python main.py trade --team1 "Alex" --team2 "Edmund" -n 2 -t 0.1

echo "Processing completed. Container will exit."
