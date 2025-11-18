"""
Player Availability Prediction Library (Batch Processing)

Functions:
- predict_all_player_probabilities: Calculates play probability for all players.
- generate_weighted_game_samples: Generates a random sample of games.
- save_predictions_to_db: Saves probabilities to DB as aggregate stats (season=1).
"""

import random
from typing import List, Tuple, Dict
from sqlalchemy.orm import Session
from sqlalchemy import func, case, or_

# Imports models directly from your database file
try:
    from database import GameStats, Player, PlayerSeasonValue
except ImportError:
    print("Error: Could not import models from database.py.")
    GameStats = None
    Player = None
    PlayerSeasonValue = None

# Constant for the "Aggregate/Predictive" season ID
PREDICTIVE_SEASON_ID = 1

def predict_all_player_probabilities(
    session: Session,
    year_weights: List[Tuple[int, float]],
    prior_play_percentage: float = 0.85,
    prior_strength_in_games: float = 82.0
) -> Dict[str, float]:
    """
    Calculates the play probability for all players in one batch.
    Returns: {player_id (string): probability}
    """
    
    # 1. Calculate Bayesian prior parameters
    prior_played = prior_play_percentage * prior_strength_in_games
    prior_strength = prior_strength_in_games

    seasons_to_query = [season for season, weight in year_weights]
    weights_map = dict(year_weights)
    
    if not GameStats or not Player:
        raise ImportError("Database models not loaded.")
    
    # 2. Query: Get aggregated stats for all players
    # Count a game as NOT played if MP is None, empty, or "00:00"
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
                Player.player_id, # String ID
                GameStats.season,
                func.count(GameStats.id).label('total_games'),
                func.sum(played_game_case).label('games_played')
            )
            .join(Player, Player.id == GameStats.player_id)
            .filter(GameStats.season.in_(seasons_to_query))
            .group_by(Player.player_id, GameStats.season)
            .all()
        )
    except Exception as e:
        print(f"Database query for stats failed: {e}")
        return {}

    # 3. Process stats into "virtual seasons"
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

    # 4. Query: Get all distinct STRING player IDs
    try:
        all_players_query = session.query(Player.player_id).distinct().all()
    except Exception as e:
        print(f"Database query for all players failed: {e}")
        return {}

    # 5. Calculate final probabilities
    final_probabilities = {}
    
    for (string_player_id,) in all_players_query:
        player_data = virtual_stats.get(string_player_id)
        
        if player_data and player_data['weight'] > 0:
            # Normalize and apply Bayesian smoothing
            virtual_gp = player_data['gp'] / player_data['weight']
            virtual_tg = player_data['tg'] / player_data['weight']

            final_numerator = virtual_gp + prior_played
            final_denominator = virtual_tg + prior_strength
            prob = final_numerator / final_denominator
        else:
            # Rookie/No data -> Prior
            prob = prior_play_percentage
            
        final_probabilities[string_player_id] = max(0.0, min(1.0, prob))

    return final_probabilities

def save_predictions_to_db(session: Session, all_probs: Dict[str, float]):
    """
    Saves calculated probabilities to PlayerSeasonValue table.
    Sets season=1 (PREDICTIVE_SEASON_ID) for all entries.
    Uses session.merge() to override existing data.
    """
    print(f"\nSaving {len(all_probs)} predictions to database (Season={PREDICTIVE_SEASON_ID})...")
    
    if not PlayerSeasonValue:
        print("Error: PlayerSeasonValue model not loaded.")
        return

    try:
        # Map string_player_id -> integer_player_id (Primary Key)
        player_map = {
            p.player_id: p.id 
            for p in session.query(Player.id, Player.player_id).all()
        }
        
        count = 0
        for string_pid, prob in all_probs.items():
            int_pid = player_map.get(string_pid)
            
            if int_pid:
                # Create object with season=1
                psv = PlayerSeasonValue(
                    player_id=int_pid,
                    season=PREDICTIVE_SEASON_ID, 
                    play_likelihood=prob
                )
                # Merge updates if (player_id, season) exists, inserts if not
                session.merge(psv)
                count += 1
            else:
                # This might happen if a player ID exists in GameStats but not Players table (rare)
                pass

        session.commit()
        print(f"Successfully saved/updated {count} records.")
        
    except Exception as e:
        print(f"Error saving predictions to database: {e}")
        session.rollback()

def _create_dummy_game() -> GameStats:
    """Returns a zeroed-out GameStats object (DNP)."""
    return GameStats(
        minutes_played="00:00", game_started=0,
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
    availability_predictions: Dict[str, float],
    include_dummy_games: bool = True
) -> Dict[str, List[GameStats]]:
    """
    Generates a weighted, random sample of past games for a list of players.

    Args:
        session: The active SQLAlchemy session.
        player_ids: A list of STRING player_ids (e.g., ['curryst01', ...]).
        num_games: The number of games to generate *per player*.
        year_weights: List of (season, weight) tuples.
        availability_predictions: Dict of {player_id: probability}. Only
                                  used if include_dummy_games is True.
        include_dummy_games (bool): If True, factors in availability_predictions
                                    to randomly return dummy games.
                                    If False, only returns real played games.

    Returns:
        A dictionary mapping {player_id: [list of GameStats objects]}.
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
            # Step A: Check if we should generate a dummy game
            if include_dummy_games and random.random() > availability_prob:
                # UNAVAILABLE: Add a dummy game
                final_samples[player_id].append(_create_dummy_game())
            else:
                # AVAILABLE (or include_dummy_games is False): Sample a real game
                if pool and pool['games']:
                    selected_game = random.choices(
                        pool['games'], 
                        weights=pool['weights'], 
                        k=1
                    )[0]
                    final_samples[player_id].append(selected_game)
                else:
                    # No historical games to sample from. Return a dummy game.
                    final_samples[player_id].append(_create_dummy_game())
                    
    return final_samples