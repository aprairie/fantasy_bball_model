"""
Player Availability Prediction Library (Batch Processing)

Calculates play probability for all players by importing
models directly from the 'database.py' file.
"""

from typing import List, Tuple, Dict
from sqlalchemy.orm import Session
from sqlalchemy import func, case, or_

# Imports models directly from your database file
try:
    from database import GameStats, Player
except ImportError:
    print("Error: Could not import 'GameStats' or 'Player' from database.py.")
    print("Please ensure 'database.py' is in the same directory.")
    GameStats = None # Will cause a failure if not imported
    Player = None


def predict_all_player_probabilities(
    session: Session,
    year_weights: List[Tuple[int, float]],
    prior_play_percentage: float = 0.85,
    prior_strength_in_games: float = 82.0
) -> Dict[str, float]:
    """
    Calculates the play probability for all players in one batch.

    Args:
        session: The active SQLAlchemy session object.
        year_weights: A list of (season, weight) tuples.
                      e.g., [(2024, 0.6), (2023, 0.3), (2022, 0.1)]
        prior_play_percentage (float): The "league average" probability
                                       of playing (e.g., 0.85 for 85%).
        prior_strength_in_games (float): The "strength" of the prior,
                                         in equivalent games (e.g., 82).
    Returns:
        A dictionary of {player_id (string): probability} for all players.
    """
    
    # 1. Calculate Bayesian prior parameters (alpha and beta)
    prior_played = prior_play_percentage * prior_strength_in_games
    prior_missed = (1.0 - prior_play_percentage) * prior_strength_in_games
    prior_strength = prior_strength_in_games

    seasons_to_query = [season for season, weight in year_weights]
    weights_map = dict(year_weights)
    
    if not GameStats or not Player:
        raise ImportError("Database models 'GameStats' or 'Player' not loaded.")
    
    # 2. Query 1: Get aggregated stats for all players
    
    # Define a "played game" case. Your 'minutes_played' is a String.
    # We count a game as NOT played if MP is None, empty string, or "00:00".
    # Anything else counts as played (1).
    
    # --- FIXED: Removed the outer list '[]' ---
    played_game_case = case(
        (or_(
            GameStats.minutes_played.is_(None),
            GameStats.minutes_played == "",
            GameStats.minutes_played == "00:00"
        ), 0), # 0 games played if DNP
        else_=1 # 1 game played otherwise
    )
    # --- END FIX ---

    try:
        stats_query = (
            session.query(
                Player.player_id, # Get the STRING player_id (e.g., 'curryst01')
                GameStats.season,
                func.count(GameStats.id).label('total_games'),
                func.sum(played_game_case).label('games_played')
            )
            .join(Player, Player.id == GameStats.player_id) # Join on the integer PK/FK
            .filter(GameStats.season.in_(seasons_to_query))
            .group_by(Player.player_id, GameStats.season)
            .all()
        )
    except Exception as e:
        print(f"Database query for stats failed: {e}")
        return {}

    # 3. Process stats into "virtual seasons" for each player
    # virtual_stats = {string_player_id: {'gp': 0.0, 'tg': 0.0, 'weight': 0.0}}
    virtual_stats = {}
    
    for string_player_id, season, tg, gp in stats_query:
        weight = weights_map.get(season, 0)
        if weight > 0:
            stats = virtual_stats.setdefault(
                string_player_id, 
                {'gp': 0.0, 'tg': 0.0, 'weight': 0.0}
            )
            stats['gp'] += gp * weight
            stats['tg'] += tg * weight
            stats['weight'] += weight

    # 4. Query 2: Get all distinct STRING player IDs from the Player table
    try:
        all_players_query = session.query(Player.player_id).distinct().all()
    except Exception as e:
        print(f"Database query for all players failed: {e}")
        return {}

    # 5. Calculate final probabilities for ALL players
    final_probabilities = {}
    
    for (string_player_id,) in all_players_query:
        player_data = virtual_stats.get(string_player_id)
        
        if player_data and player_data['weight'] > 0:
            # Player has data in the weighted seasons.
            # Normalize the weighted stats to get the "virtual" season
            virtual_gp = player_data['gp'] / player_data['weight']
            virtual_tg = player_data['tg'] / player_data['weight']

            # Apply Bayesian smoothing
            final_numerator = virtual_gp + prior_played
            final_denominator = virtual_tg + prior_strength
            
            prob = final_numerator / final_denominator
            
        else:
            # Player is a rookie or has no data in the weighted seasons.
            # Use the prior probability directly.
            prob = prior_play_percentage
            
        final_probabilities[string_player_id] = max(0.0, min(1.0, prob))

    return final_probabilities