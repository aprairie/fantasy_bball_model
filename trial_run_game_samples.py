"""
Executable script to calculate, save, and print the play probability
for every player in the database.

It also now runs a test simulation for a few players.
"""

import sys
from sqlalchemy.orm import Session
from typing import Dict

# --- Library Import ---
try:
    from game_picker_lib import (
        predict_all_player_probabilities,
        generate_weighted_game_samples # <-- IMPORT NEW FUNCTION
    )
except ImportError:
    print("Error: Could not find 'player_availability_all.py'.")
    print("Please make sure that file is in the same directory.")
    sys.exit(1)

# --- Database Setup ---
try:
    from database import SessionLocal, init_db, Player, PlayerSeasonValue
except ImportError:
    print("Error: Could not import from 'database.py'.")
    print("Please ensure 'database.py' is in the same directory.")
    sys.exit(1)

# --- Model Parameters ---
YEAR_WEIGHTS = [(2025, 0.6), (2024, 0.3), (2023, 0.1)]
CURRENT_PREDICTION_SEASON = 2026
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
        player_map = {
            p.player_id: p.id 
            for p in session.query(Player.id, Player.player_id).all()
        }
        
        for string_pid, prob in all_probs.items():
            int_pid = player_map.get(string_pid)
            
            if int_pid:
                psv = PlayerSeasonValue(
                    player_id=int_pid,
                    season=season_to_update,
                    play_likelihood=prob
                )
                session.merge(psv)
            else:
                print(f"Warning: Player '{string_pid}' in probs not found in Player table.")

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
    all_probs = {}
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

    # --- 4. Print Availability Results (Top 50) ---
    if all_probs:
        print("\n--- Player Availability Predictions (Top 50) ---")
        sorted_players = sorted(
            all_probs.items(), 
            key=lambda item: item[1], 
            reverse=True
        )
        for player_id, probability in sorted_players[:50]:
            print(f"{player_id:<12} | {probability:>8.2%}")
        if len(sorted_players) > 50:
            print(f"... and {len(sorted_players) - 50} more.")
            
    # --- 5. NEW: Run and Print Game Simulation ---
    if all_probs:
        print("\n--- Running Game Sample Simulation (First 20 Players) ---")
        
        # Get the first 20 players from the probability list
        players_to_simulate = [pid for pid, prob in sorted_players[:20]]
        num_games_to_simulate = 5 # Generate 5 games for each
        
        if players_to_simulate:
            game_samples = generate_weighted_game_samples(
                session,
                players_to_simulate,
                num_games_to_simulate,
                YEAR_WEIGHTS,
                all_probs
            )
            
            # Print the simulation results
            for player_id, games in game_samples.items():
                prob = all_probs[player_id]
                print(f"\n  Player: {player_id} (Availability: {prob:.2%})")
                
                for i, game in enumerate(games):
                    if game.minutes_played == "00:00":
                        print(f"    Game {i+1}: [DNP - MISSED GAME]")
                    else:
                        print(f"    Game {i+1}: [PLAYED] Pts: {game.points}, Reb: {game.total_rebounds}, Ast: {game.assists} (from {game.game_date})")
        else:
            print("No players available to simulate.")

    # --- 6. Clean up ---
    session.close()
    print("\nDone.")


if __name__ == "__main__":
    main()