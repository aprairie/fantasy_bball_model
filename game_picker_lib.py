"""
Player Availability Prediction Library (Batch Processing)

Functions:
- predict_all_player_probabilities: Calculates play probability for all players.
- generate_weighted_game_samples: Generates a random sample of games.
- save_predictions_to_db: Saves probabilities to DB as aggregate stats (season=1).
- calculate_all_player_values: Runs the full pipeline to generate Season 1 (Healthy) and Season 2 (Risk-Adjusted).
"""

import random
import numpy as np
import time
from typing import List, Tuple, Dict, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, case, or_, select, exc
from sqlalchemy.dialects.postgresql import insert as pg_insert
from database import GameStats, Player, PlayerSeasonValue
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constant for the "Aggregate/Predictive" season ID
PREDICTIVE_SEASON_ID = 1
RISK_ADJUSTED_SEASON_ID = 2  # <--- NEW: Season ID for injury-adjusted stats

# --- VALUE CALCULATION CONFIGURATION ---
BENCHMARK_SEASON = 2025 
CALCULATION_SEASON = 2026
SIMULATED_SEASON_ID = 1

# Weights for Availability Calculation
AVAILABILITY_YEAR_WEIGHTS = [(2026, 1), (2025, 1), (2024, 1)]

# Weights for Game Sampling (Performance)
# SIM_YEAR_WEIGHTS = [(2026, 2.5), (2025, 0.5), (2024, 0.1)] # old, stable
SIM_YEAR_WEIGHTS = [(2026, 100), (2025, 1)]

NUM_SIM_GAMES = 1000
MIN_GAMES_PLAYED = 1 
FG_BENCHMARK = 0.48
FT_BENCHMARK = 0.81
TOP_N_PLAYERS = 200

# ---------------------------------------------------------
# EXISTING AVAILABILITY FUNCTIONS
# ---------------------------------------------------------

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

# ---------------------------------------------------------
# NEW: VALUE CALCULATION FUNCTIONS
# ---------------------------------------------------------

def get_season_stats(db: Session, season: int):
    """
    Fetches and aggregates all player stats for a specific real season.
    """
    print(f"Querying aggregated season stats for {season}...")
    
    season_stats_query = (
        select(
            GameStats.player_id,
            func.count(GameStats.id).label("games_played"),
            func.avg(GameStats.points).label("avg_pts"),
            func.avg(GameStats.total_rebounds).label("avg_reb"),
            func.avg(GameStats.assists).label("avg_ast"),
            func.avg(GameStats.steals).label("avg_stl"),
            func.avg(GameStats.blocks).label("avg_blk"),
            func.avg(GameStats.three_pointers).label("avg_tpm"),
            func.avg(GameStats.turnovers).label("avg_to"),
            func.sum(GameStats.field_goal_attempts).label("total_fga"),
            func.sum(GameStats.field_goals).label("total_fgm"),
            func.sum(GameStats.free_throw_attempts).label("total_fta"),
            func.sum(GameStats.free_throws).label("total_ftm")
        )
        .where(GameStats.season == season)
        .group_by(GameStats.player_id)
        .having(func.count(GameStats.id) >= MIN_GAMES_PLAYED)
    )
    
    results = db.execute(season_stats_query).all()
    
    stats_dict = {}
    for row in results:
        row_data = dict(row._mapping)
        
        # Explicitly cast all aggregated values to float/int
        row_data['games_played'] = int(row_data['games_played'])
        row_data['avg_pts'] = float(row_data['avg_pts'])
        row_data['avg_reb'] = float(row_data['avg_reb'])
        row_data['avg_ast'] = float(row_data['avg_ast'])
        row_data['avg_stl'] = float(row_data['avg_stl'])
        row_data['avg_blk'] = float(row_data['avg_blk'])
        row_data['avg_tpm'] = float(row_data['avg_tpm'])
        row_data['avg_to'] = float(row_data['avg_to'])
        row_data['total_fga'] = float(row_data['total_fga'] or 0)
        row_data['total_fgm'] = float(row_data['total_fgm'] or 0)
        row_data['total_fta'] = float(row_data['total_fta'] or 0)
        row_data['total_ftm'] = float(row_data['total_ftm'] or 0)
        
        stats_dict[row.player_id] = row_data
        
    print(f"Found {len(stats_dict)} players with > {MIN_GAMES_PLAYED} games.")
    return stats_dict

def get_simulated_stats(db: Session, 
                        year_weights: List[Tuple[int, float]], 
                        num_games: int,
                        include_dummy_games: bool = False,
                        availability_predictions: Dict[str, float] = {}) -> Dict[int, dict]:
    """
    Generates weighted, simulated stats for ALL players.
    Accepts parameters to control injury simulation.
    """
    mode_str = "RISK-ADJUSTED" if include_dummy_games else "HEALTHY"
    print(f"\n--- Generating {num_games} simulated games per player ({mode_str}) ---")
    
    try:
        all_players = db.query(Player.id, Player.player_id).all()
        string_to_int_map = {p.player_id: p.id for p in all_players}
        player_ids_to_sim = list(string_to_int_map.keys())
    except Exception as e:
        print(f"Error querying players: {e}")
        return {}
        
    print(f"Found {len(player_ids_to_sim)} total players to simulate.")

    # Call internal library function
    sim_results = generate_weighted_game_samples(
        session=db,
        player_ids=player_ids_to_sim,
        num_games=num_games,
        year_weights=year_weights,
        availability_predictions=availability_predictions, 
        include_dummy_games=include_dummy_games 
    )
    
    stats_dict = {}
    for string_pid, game_list in sim_results.items():
        int_pid = string_to_int_map.get(string_pid)
        if not int_pid:
            continue
        
        # For "Risk Adjusted" stats, we KEEP the dummy games (zeros) in the denominator
        # to lower the averages.
        # However, if the game is "None" (which shouldn't happen with dummy games returning 0s), check carefully.
        # Dummy games have minutes_played="00:00".
        
        # If we want per-game averages to reflect risk, we must divide by `num_games`, 
        # effectively counting the zeroes.
        # The `generate_weighted_game_samples` returns a list of size `num_games`.
        
        valid_games = [g for g in game_list if g is not None]
        
        if not valid_games:
            continue

        games_count = len(valid_games) # This should equal num_games
        
        # Sums include the zeros from dummy games
        total_pts = float(np.sum([g.points or 0 for g in valid_games]))
        total_reb = float(np.sum([g.total_rebounds or 0 for g in valid_games]))
        total_ast = float(np.sum([g.assists or 0 for g in valid_games]))
        total_stl = float(np.sum([g.steals or 0 for g in valid_games]))
        total_blk = float(np.sum([g.blocks or 0 for g in valid_games]))
        total_tpm = float(np.sum([g.three_pointers or 0 for g in valid_games]))
        total_to = float(np.sum([g.turnovers or 0 for g in valid_games]))
        
        total_fga = float(np.sum([g.field_goal_attempts or 0 for g in valid_games]))
        total_fgm = float(np.sum([g.field_goals or 0 for g in valid_games]))
        total_fta = float(np.sum([g.free_throw_attempts or 0 for g in valid_games]))
        total_ftm = float(np.sum([g.free_throws or 0 for g in valid_games]))

        stats_dict[int_pid] = {
            'games_played': games_count,
            'avg_pts': total_pts / games_count,
            'avg_reb': total_reb / games_count,
            'avg_ast': total_ast / games_count,
            'avg_stl': total_stl / games_count,
            'avg_blk': total_blk / games_count,
            'avg_tpm': total_tpm / games_count,
            'avg_to': total_to / games_count,
            'total_fga': total_fga,
            'total_fgm': total_fgm,
            'total_fta': total_fta,
            'total_ftm': total_ftm,
        }
        
    print(f"Successfully aggregated simulated stats for {len(stats_dict)} players.")
    return stats_dict

def calculate_impact_stats(all_player_stats: dict):
    """Calculates and adds the FG% and FT% "impact" stats."""
    for player_id, stats in all_player_stats.items():
        games_played = stats['games_played']
        if games_played == 0:
            games_played = 1
            
        if stats['total_fga'] > 0:
            fg_impact = (stats['total_fgm'] - (stats['total_fga'] * FG_BENCHMARK)) / games_played
        else:
            fg_impact = 0.0
            
        if stats['total_fta'] > 0:
            ft_impact = (stats['total_ftm'] - (stats['total_fta'] * FT_BENCHMARK)) / games_played
        else:
            ft_impact = 0.0
            
        stats['fg_impact'] = fg_impact
        stats['ft_impact'] = ft_impact

def get_benchmarks(player_stats_cohort: list):
    """Calculates the average (mean) for each stat category."""
    benchmarks = {
        'pts': np.mean([s['avg_pts'] for s in player_stats_cohort]) + np.finfo(float).eps,
        'reb': np.mean([s['avg_reb'] for s in player_stats_cohort]) + np.finfo(float).eps,
        'ast': np.mean([s['avg_ast'] for s in player_stats_cohort]) + np.finfo(float).eps,
        'stl': np.mean([s['avg_stl'] for s in player_stats_cohort]) + np.finfo(float).eps,
        'blk': np.mean([s['avg_blk'] for s in player_stats_cohort]) + np.finfo(float).eps,
        'tpm': np.mean([s['avg_tpm'] for s in player_stats_cohort]) + np.finfo(float).eps,
        'to': np.mean([s['avg_to'] for s in player_stats_cohort]) + np.finfo(float).eps,
        'fg_impact': np.mean([s['fg_impact'] for s in player_stats_cohort]),
        'ft_impact': np.mean([s['ft_impact'] for s in player_stats_cohort]),
    }
    return benchmarks

def calculate_normalized_scores(player_stats_dict: dict, benchmarks: dict, target_std_dev: float, impact_means: dict, impact_std_devs: dict):
    """
    Calculates the final 9-category scores for a given set of players
    based on a given set of benchmarks and variance targets.
    """
    final_scores = {}
    
    for player_id, stats in player_stats_dict.items():
        pts_score = (stats['avg_pts'] / benchmarks['pts']) * 10
        reb_score = (stats['avg_reb'] / benchmarks['reb']) * 10
        ast_score = (stats['avg_ast'] / benchmarks['ast']) * 10
        stl_score = (stats['avg_stl'] / benchmarks['stl']) * 10
        blk_score = (stats['avg_blk'] / benchmarks['blk']) * 10
        tpm_score = (stats['avg_tpm'] / benchmarks['tpm']) * 10
        to_score = (stats['avg_to'] / benchmarks['to']) * -10
        
        # Z-score = (value - mean) / std_dev
        fg_z_score = (stats['fg_impact'] - impact_means['fg']) / impact_std_devs['fg']
        ft_z_score = (stats['ft_impact'] - impact_means['ft']) / impact_std_devs['ft']
        
        # Scale Z-score to match the variance of counting stats
        fg_pct_score = fg_z_score * target_std_dev
        ft_pct_score = ft_z_score * target_std_dev
        
        total_score = (
            pts_score + reb_score + ast_score + stl_score + 
            blk_score + tpm_score + to_score + fg_pct_score + ft_pct_score
        )
        
        final_scores[player_id] = {
            'pts_score': pts_score,
            'reb_score': reb_score,
            'ast_score': ast_score,
            'stl_score': stl_score,
            'blk_score': blk_score,
            'tpm_score': tpm_score,
            'fg_pct_score': fg_pct_score,
            'ft_pct_score': ft_pct_score,
            'to_score': to_score,
            'total_score': total_score,
            'play_likelihood': 0.0 # To be populated by availability logic later if needed
        }
    return final_scores

def upsert_season_values(db: Session, values_to_upsert: list, target_season: int):
    """
    Efficiently inserts or updates PlayerSeasonValue records for a SPECIFIC season.
    """
    if not values_to_upsert:
        print(f"No values to upsert for season {target_season}.")
        return
        
    print(f"Upserting {len(values_to_upsert)} records for season {target_season}...")
    
    table = PlayerSeasonValue.__table__
    stmt = pg_insert(table).values(values_to_upsert)
    
    # Do not update 'play_likelihood' as it's set by the other script
    update_columns = {
        col.name: col
        for col in stmt.excluded
        if not col.primary_key and col.name != 'play_likelihood'
    }
    
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=['player_id', 'season'],
        set_=update_columns
    )
    
    try:
        db.execute(upsert_stmt)
        db.commit()
        print(f"Successfully upserted records for season {target_season}.")
    except exc.SQLAlchemyError as e:
        print(f"[!!!] Database upsert failed: {e}")
        db.rollback()
        print("--- Rolled back changes. ---")

def calculate_all_player_values(session: Session):
    """
    Runs the full pipeline:
    1. Benchmarks against BENCHMARK_SEASON (2025).
    2. Calculates 'Real' scores for CALCULATION_SEASON (2026).
    3. Calculates 'Simulated Healthy' scores for SIMULATED_SEASON_ID (1).
    4. Calculates 'Simulated Risk-Adjusted' scores for RISK_ADJUSTED_SEASON_ID (2).
    """
    print(f"--- Starting Player Value Calculation ---")
    start_time = time.time()
    logger.info('starting calc all player value')
    
    try:
        # ==================================================================
        # PASS 0: PREDICT AVAILABILITY
        # We need these probabilities for the Risk-Adjusted simulation (Season 2)
        # ==================================================================
        logger.info(f"Calculating availability probabilities...")
        availability_probs = predict_all_player_probabilities(
            session, 
            AVAILABILITY_YEAR_WEIGHTS
        )
        
        # ==================================================================
        # PASS 1: CALCULATE BENCHMARKS
        # We use BENCHMARK_SEASON (e.g., 2025) to establish what "Average" looks like.
        # ==================================================================
        logger.info(f"\n--- Pass 1: Analyzing Benchmark Season " + str(BENCHMARK_SEASON))
        
        benchmark_stats_all = get_season_stats(session, BENCHMARK_SEASON)
        if not benchmark_stats_all:
            logger.info("No stats found for benchmark season. Exiting.")
            return
            
        calculate_impact_stats(benchmark_stats_all)
        provisional_benchmarks = get_benchmarks(list(benchmark_stats_all.values()))
        
        # Find Top 200 to create a "Standard League" baseline
        provisional_scores = calculate_normalized_scores(
            benchmark_stats_all, provisional_benchmarks, 1.0, 
            {'fg': 0.0, 'ft': 0.0}, {'fg': 1.0, 'ft': 1.0}
        )
        
        sorted_players = sorted(
            provisional_scores.items(), 
            key=lambda item: item[1]['total_score'], 
            reverse=True
        )
        top_n_ids = {pid for pid, score in sorted_players[:TOP_N_PLAYERS]}
        
        final_cohort_stats = [
            stats for pid, stats in benchmark_stats_all.items() if pid in top_n_ids
        ]
        
        if not final_cohort_stats:
            print("Error creating benchmark cohort.")
            return

        # Calculate Final Benchmarks and Variances
        final_benchmarks = get_benchmarks(final_cohort_stats)
        
        pts_scores_cohort = [(s['avg_pts']/final_benchmarks['pts'])*10 for s in final_cohort_stats]
        target_std_dev = np.std(pts_scores_cohort)
        
        fg_imp = [s['fg_impact'] for s in final_cohort_stats]
        ft_imp = [s['ft_impact'] for s in final_cohort_stats]
        
        impact_means = {'fg': np.mean(fg_imp), 'ft': np.mean(ft_imp)}
        impact_std_devs = {
            'fg': np.std(fg_imp) + np.finfo(float).eps,
            'ft': np.std(ft_imp) + np.finfo(float).eps
        }

        logger.info('Benchmarks calculated')
        print(f"Benchmarks calculated. StdDev Target: {target_std_dev:.4f}")


        # ==================================================================
        # PASS 2: CALCULATE REAL SEASON STATS (Normal)
        # Calculate scores for the current actual season (e.g., 2026)
        # ==================================================================
        print(f"\n--- Pass 2: Calculating Normal Scores for {CALCULATION_SEASON} ---")
        logger.info('Calcuating normal scores for ' + str(CALCULATION_SEASON))
        
        normal_stats = get_season_stats(session, CALCULATION_SEASON)
        
        if normal_stats:
            calculate_impact_stats(normal_stats)
            
            normal_scores = calculate_normalized_scores(
                normal_stats, final_benchmarks, target_std_dev, impact_means, impact_std_devs
            )
            
            values_normal = []
            for player_id, scores in normal_scores.items():
                values_normal.append({
                    'player_id': player_id,
                    'season': CALCULATION_SEASON, # <--- Stored as 2026
                    **scores
                })
            
            upsert_season_values(session, values_normal, CALCULATION_SEASON)
        else:
            print(f"No data found for {CALCULATION_SEASON}, skipping normal calculation.")


        # ==================================================================
        # PASS 3: CALCULATE SIMULATED STATS (Healthy / True Talent)
        # Season ID = 1
        # ==================================================================
        print(f"\n--- Pass 3: Calculating Simulated (Healthy) Scores for Season {SIMULATED_SEASON_ID} ---")
        logger.info('Calcuating simulated scores for ' + str(SIMULATED_SEASON_ID))
        
        sim_stats_healthy = get_simulated_stats(
            session, 
            SIM_YEAR_WEIGHTS, 
            NUM_SIM_GAMES,
            include_dummy_games=False  # <--- Healthy
        )
        
        if sim_stats_healthy:
            calculate_impact_stats(sim_stats_healthy)
            
            sim_scores = calculate_normalized_scores(
                sim_stats_healthy, final_benchmarks, target_std_dev, impact_means, impact_std_devs
            )
            
            values_sim = []
            for player_id, scores in sim_scores.items():
                values_sim.append({
                    'player_id': player_id,
                    'season': SIMULATED_SEASON_ID, # <--- Stored as 1
                    **scores
                })
            
            upsert_season_values(session, values_sim, SIMULATED_SEASON_ID)
        else:
            print("No simulated stats generated.")
            logger.info('no simulated stats generated')

        # ==================================================================
        # PASS 4: CALCULATE SIMULATED STATS (Risk-Adjusted)
        # Season ID = 2
        # ==================================================================
        print(f"\n--- Pass 4: Calculating Simulated (Risk-Adjusted) Scores for Season {RISK_ADJUSTED_SEASON_ID} ---")
        logger.info('Calcuating simulated scores for ' + str(RISK_ADJUSTED_SEASON_ID))
        
        sim_stats_risky = get_simulated_stats(
            session, 
            SIM_YEAR_WEIGHTS, 
            NUM_SIM_GAMES,
            include_dummy_games=True, # <--- Risk Adjusted (Injuries Enabled)
            availability_predictions=availability_probs
        )
        
        if sim_stats_risky:
            calculate_impact_stats(sim_stats_risky)
            
            sim_scores_risky = calculate_normalized_scores(
                sim_stats_risky, final_benchmarks, target_std_dev, impact_means, impact_std_devs
            )
            
            values_sim_risky = []
            for player_id, scores in sim_scores_risky.items():
                values_sim_risky.append({
                    'player_id': player_id,
                    'season': RISK_ADJUSTED_SEASON_ID, # <--- Stored as 2
                    **scores
                })
            
            upsert_season_values(session, values_sim_risky, RISK_ADJUSTED_SEASON_ID)
        else:
            print("No risk-adjusted stats generated.")
            
    except Exception as e:
        print(f"An unexpected error occurred during value calculation: {e}")
        import traceback
        traceback.print_exc()
    finally:
        end_time = time.time()
        print(f"\n--- Calculation finished in {end_time - start_time:.2f} seconds. ---")