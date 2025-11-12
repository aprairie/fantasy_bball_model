"""
Executable script to calculate, save, and print the play probability
for every player in the database.
"""

import sys
from sqlalchemy.orm import Session
from typing import Dict

# --- Library Import ---
try:
    from game_picker_lib import predict_all_player_probabilities
except ImportError:
    print("Error: Could not find 'player_availability_all.py'.")
    print("Please make sure that file is in the same directory.")
    sys.exit(1)

# --- Database Setup ---
try:
    # Import all necessary components from your database file
    from database import SessionLocal, init_db, Player, PlayerSeasonValue
except ImportError:
    print("Error: Could not import from 'database.py'.")
    print("Please ensure 'database.py' is in the same directory.")
    sys.exit(1)

# --- Model Parameters ---

# Define the (season, weight) pairs.
# 60% for 2025, 30% for 2024, 10% for 2023
# NOTE: The 2025 season is the 2024-25 season.
# Today is Nov 2025, so the 2026 season (2025-26) is active.
# We'll use 2025, 2024, 2023 as the three most *completed* seasons.
# (Adjust this list as needed for your data)
YEAR_WEIGHTS = [(2025, 0.6), (2024, 0.3), (2023, 0.1)]

# This is the season we are generating predictions FOR.
# Since it's Nov 2025, we are predicting for the 2025-26 season,
# which is stored as 2026.
CURRENT_PREDICTION_SEASON = 2026

# Define the priors.
PRIOR_PLAY_PERCENTAGE = 0.85
PRIOR_STRENGTH_IN_GAMES = 82.0


def save_predictions_to_db(session: Session, 
                           all_probs: Dict[str, float], 
                           season_to_update: int):
    """
    Saves the calculated probabilities to the PlayerSeasonValue table.
    """
    print(f"\nSaving {len(all_probs)} predictions to database for season {season_to_update}...")
    
    try:
        # Get a map of {string_player_id: integer_player_id}
        player_map = {
            p.player_id: p.id 
            for p in session.query(Player.id, Player.player_id).all()
        }
        
        objects_to_merge = []
        for string_pid, prob in all_probs.items():
            int_pid = player_map.get(string_pid)
            
            if int_pid:
                # Create a PlayerSeasonValue object.
                # 'session.merge()' will either INSERT a new record (if PK doesn't exist)
                # or UPDATE the 'play_likelihood' on the existing record.
                psv = PlayerSeasonValue(
                    player_id=int_pid,
                    season=season_to_update,
                    play_likelihood=prob
                )
                # We merge here, but committing in a batch is faster.
                session.merge(psv)
            else:
                print(f"Warning: Player '{string_pid}' in probs not found in Player table.")

        # Commit all changes at once
        session.commit()
        print("Save complete.")
        
    except Exception as e:
        print(f"Error saving predictions to database: {e}")
        session.rollback()


def main():
    """
    Main executable function.
    """
    print("--- Running Player Availability Predictions ---")
    
    # --- 1. Setup Database Connection ---
    print("Initializing database connection...")
    try:
        init_db() # Creates tables if they don't exist
        session = SessionLocal()
        print("Database connected.")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

    # --- 2. Run Predictions ---
    print(f"Calculating probabilities using weights for seasons: {YEAR_WEIGHTS}")
    try:
        all_probs = predict_all_player_probabilities(
            session=session,
            year_weights=YEAR_WEIGHTS,
            prior_play_percentage=PRIOR_PLAY_PERCENTAGE,
            prior_strength_in_games=PRIOR_STRENGTH_IN_GAMES
        )
    except Exception as e:
        print(f"An error occurred during prediction: {e}")
        session.close()
        sys.exit(1)

    print(f"Calculation complete. Processed {len(all_probs)} players.")
    
    # --- 3. Save Results to Database ---
    if all_probs:
        save_predictions_to_db(session, all_probs, CURRENT_PREDICTION_SEASON)
    else:
        print("No players found or processed.")

    # --- 4. Print Results (as requested) ---
    if all_probs:
        print("\n--- Player Availability Predictions (All players) ---")
        
        # Sort by probability (descending) for a more useful list
        sorted_players = sorted(
            all_probs.items(), 
            key=lambda item: item[1], 
            reverse=True
        )
        
        for player_id, probability in sorted_players:
            print(f"{player_id:<12} | {probability:>8.2%}")
        
    # --- 5. Clean up ---
    session.close()
    print("\nDone.")


if __name__ == "__main__":
    main()