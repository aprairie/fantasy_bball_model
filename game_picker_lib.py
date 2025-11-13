"""
Player Availability Prediction Library (Batch Processing)

Functions:
- predict_all_player_probabilities: Calculates play probability for all players.
- generate_weighted_game_samples: Generates a random sample of games for
                                  a list of players, factoring in availability.
"""

import random
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
    
    played_game_case = case(
        (or_(
            GameStats.minutes_played.is_(None),
            GameStats.minutes_played == "",
            GameStats.minutes_played == "00:00"
        ), 0), # 0 games played if DNP
        else_=1 # 1 game played otherwise
    )

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

# --- NEW FUNCTION ---

def _create_dummy_game() -> GameStats:
    """
    Returns a new GameStats object with all stats zeroed out,
    representing a "Did Not Play" (DNP) game.
    """
    return GameStats(
        minutes_played="00:00",
        game_started=0,
        field_goals=0, field_goal_attempts=0, field_goal_pct=0.0,
        three_pointers=0, three_point_attempts=0, three_point_pct=0.0,
        free_throws=0, free_throw_attempts=0, free_throw_pct=0.0,
        offensive_rebounds=0, defensive_rebounds=0, total_rebounds=0,
        assists=0, steals=0, blocks=0, turnovers=0,
        personal_fouls=0, points=0, game_score_stat=0.0, plus_minus=0
    )

def generate_weighted_game_samples(
    session: Session,
    player_ids: List[str],
    num_games: int,
    year_weights: List[Tuple[int, float]],
    availability_predictions: Dict[str, float]
) -> Dict[str, List[GameStats]]:
    """
    Generates a weighted, random sample of past games for a list of players.

    This function factors in:
    1. Per-game weighting from 'year_weights'.
    2. Availability predictions to randomly return "dummy" 0-stat games.
    
    Args:
        session: The active SQLAlchemy session.
        player_ids: A list of STRING player_ids (e.g., ['curryst01', ...]).
        num_games: The number of games to generate *per player*.
        year_weights: List of (season, weight) tuples.
        availability_predictions: Dict of {player_id: probability} from
                                  the predict_all_player_probabilities function.

    Returns:
        A dictionary mapping {player_id: [list of GameStats objects]}.
        The list will contain 'num_games' objects for each player.
    """
    
    seasons_to_query = [season for season, weight in year_weights]
    weights_map = dict(year_weights)
    
    if not GameStats or not Player:
        raise ImportError("Database models 'GameStats' or 'Player' not loaded.")

    # 1. Define "played" game criteria (not DNP)
    played_game_filter = or_(
        GameStats.minutes_played.is_(None),
        GameStats.minutes_played == "",
        GameStats.minutes_played == "00:00"
    )

    # 2. Query all eligible "played" games for all players at once
    try:
        query = (
            session.query(Player.player_id, GameStats)
            .join(Player, Player.id == GameStats.player_id)
            .filter(Player.player_id.in_(player_ids))
            .filter(GameStats.season.in_(seasons_to_query))
            .filter(~played_game_filter)  # <-- Note the '~' for NOT
            .all()
        )
    except Exception as e:
        print(f"Database query for game samples failed: {e}")
        return {}

    # 3. Build the weighted "game pools" for each player
    # player_game_pools = {player_id: {'games': [], 'weights': []}}
    player_game_pools = {}
    
    for string_player_id, game_stat_obj in query:
        season_weight = weights_map.get(game_stat_obj.season, 0)
        
        # Only add games from seasons that have a weight > 0
        if season_weight > 0:
            pool = player_game_pools.setdefault(
                string_player_id,
                {'games': [], 'weights': []}
            )
            pool['games'].append(game_stat_obj)
            pool['weights'].append(season_weight)

    # 4. Generate the samples for each player
    final_samples = {}
    default_prob = 0.85 # Fallback if player not in availability dict

    for player_id in player_ids:
        final_samples[player_id] = []
        availability_prob = availability_predictions.get(player_id, default_prob)
        pool = player_game_pools.get(player_id)

        for _ in range(num_games):
            # Step A: Check if the player is "available"
            if random.random() > availability_prob:
                # UNAVAILABLE: Add a dummy game
                final_samples[player_id].append(_create_dummy_game())
            else:
                # AVAILABLE: Sample a real game
                if pool and pool['games']:
                    # Use random.choices to pick 1 game, respecting weights
                    selected_game = random.choices(
                        pool['games'], 
                        weights=pool['weights'], 
                        k=1
                    )[0]
                    final_samples[player_id].append(selected_game)
                else:
                    # Available, but no historical games to sample from.
                    # Treat as a DNP.
                    final_samples[player_id].append(_create_dummy_game())
                    
    return final_samples