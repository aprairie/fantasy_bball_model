import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import func, select, exc
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime
import time
from typing import List, Dict, Tuple

# Import models from database.py
from database import (
    SessionLocal,
    Player,
    EloStats,
    GameStats,
    PlayerSeasonValue
)

# Import from our simulation library
from game_picker_lib import generate_weighted_game_samples


# --- SCRIPT CONFIGURATION ---

# 1. Benchmark Configuration
# The season used to establish the "average" baseline (1.0 z-score)
BENCHMARK_SEASON = 2025 

# 2. "Normal" Calculation Configuration
# The actual current season to calculate real-world stats for
CALCULATION_SEASON = 2026

# 3. "Simulated" Calculation Configuration
# The special season ID to store simulated aggregate stats under
SIMULATED_SEASON_ID = 1
# Weights for generating the simulated stats
SIM_YEAR_WEIGHTS = [(2026, 0.6), (2025, 0.2), (2024, 0.1)]
# Number of "healthy" games to simulate per player
NUM_SIM_GAMES = 10000

MIN_GAMES_PLAYED = 1 
FG_BENCHMARK = 0.48
FT_BENCHMARK = 0.81
TOP_N_PLAYERS = 200

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
                        num_games: int) -> Dict[int, dict]:
    """
    Generates weighted, simulated stats for ALL players.
    """
    print(f"\n--- Generating {num_games} simulated games per player ---")
    print(f"Using weights: {year_weights}")
    
    try:
        all_players = db.query(Player.id, Player.player_id).all()
        string_to_int_map = {p.player_id: p.id for p in all_players}
        player_ids_to_sim = list(string_to_int_map.keys())
    except Exception as e:
        print(f"Error querying players: {e}")
        return {}
        
    print(f"Found {len(player_ids_to_sim)} total players to simulate.")

    # Call library function with include_dummy_games=False for "healthy" stats
    sim_results = generate_weighted_game_samples(
        session=db,
        player_ids=player_ids_to_sim,
        num_games=num_games,
        year_weights=year_weights,
        availability_predictions={}, 
        include_dummy_games=False 
    )
    
    stats_dict = {}
    for string_pid, game_list in sim_results.items():
        int_pid = string_to_int_map.get(string_pid)
        if not int_pid:
            continue
            
        played_games = [g for g in game_list if g.minutes_played not in (None, "", "00:00")]
        
        if not played_games:
            avg_pts, avg_reb, avg_ast, avg_stl, avg_blk, avg_tpm, avg_to = 0,0,0,0,0,0,0
            total_fga, total_fgm, total_fta, total_ftm = 0,0,0,0
            games_played = 0
        else:
            games_played = len(played_games) 
            avg_pts = float(np.mean([g.points for g in played_games]))
            avg_reb = float(np.mean([g.total_rebounds for g in played_games]))
            avg_ast = float(np.mean([g.assists for g in played_games]))
            avg_stl = float(np.mean([g.steals for g in played_games]))
            avg_blk = float(np.mean([g.blocks for g in played_games]))
            avg_tpm = float(np.mean([g.three_pointers for g in played_games]))
            avg_to = float(np.mean([g.turnovers for g in played_games]))
            
            total_fga = float(np.sum([g.field_goal_attempts for g in played_games]))
            total_fgm = float(np.sum([g.field_goals for g in played_games]))
            total_fta = float(np.sum([g.free_throw_attempts for g in played_games]))
            total_ftm = float(np.sum([g.free_throws for g in played_games]))

        stats_dict[int_pid] = {
            'games_played': games_played,
            'avg_pts': avg_pts,
            'avg_reb': avg_reb,
            'avg_ast': avg_ast,
            'avg_stl': avg_stl,
            'avg_blk': avg_blk,
            'avg_tpm': avg_tpm,
            'avg_to': avg_to,
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
        pts_score = (stats['avg_pts'] / benchmarks['pts']) * 100
        reb_score = (stats['avg_reb'] / benchmarks['reb']) * 100
        ast_score = (stats['avg_ast'] / benchmarks['ast']) * 100
        stl_score = (stats['avg_stl'] / benchmarks['stl']) * 100
        blk_score = (stats['avg_blk'] / benchmarks['blk']) * 100
        tpm_score = (stats['avg_tpm'] / benchmarks['tpm']) * 100
        to_score = (stats['avg_to'] / benchmarks['to']) * -100
        
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
            'play_likelihood': 0.0 # To be populated by run_predictions.py
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


def main():
    print(f"--- Starting Player Value Calculation ---")
    start_time = time.time()
    db: Session = SessionLocal()
    
    try:
        # ==================================================================
        # PASS 1: CALCULATE BENCHMARKS
        # We use BENCHMARK_SEASON (e.g., 2025) to establish what "Average" looks like.
        # ==================================================================
        print(f"\n--- Pass 1: Analyzing Benchmark Season {BENCHMARK_SEASON} ---")
        
        benchmark_stats_all = get_season_stats(db, BENCHMARK_SEASON)
        if not benchmark_stats_all:
            print(f"No stats found for benchmark season. Exiting.")
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
        
        pts_scores_cohort = [(s['avg_pts']/final_benchmarks['pts'])*100 for s in final_cohort_stats]
        target_std_dev = np.std(pts_scores_cohort)
        
        fg_imp = [s['fg_impact'] for s in final_cohort_stats]
        ft_imp = [s['ft_impact'] for s in final_cohort_stats]
        
        impact_means = {'fg': np.mean(fg_imp), 'ft': np.mean(ft_imp)}
        impact_std_devs = {
            'fg': np.std(fg_imp) + np.finfo(float).eps,
            'ft': np.std(ft_imp) + np.finfo(float).eps
        }
        
        print(f"Benchmarks calculated. StdDev Target: {target_std_dev:.4f}")


        # ==================================================================
        # PASS 2: CALCULATE REAL SEASON STATS (Normal)
        # Calculate scores for the current actual season (e.g., 2026)
        # ==================================================================
        print(f"\n--- Pass 2: Calculating Normal Scores for {CALCULATION_SEASON} ---")
        
        normal_stats = get_season_stats(db, CALCULATION_SEASON)
        
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
            
            upsert_season_values(db, values_normal, CALCULATION_SEASON)
        else:
            print(f"No data found for {CALCULATION_SEASON}, skipping normal calculation.")


        # ==================================================================
        # PASS 3: CALCULATE SIMULATED STATS (Aggregate)
        # Calculate scores based on 1000 simulated "healthy" games
        # ==================================================================
        print(f"\n--- Pass 3: Calculating Simulated Scores for Season {SIMULATED_SEASON_ID} ---")
        
        sim_stats = get_simulated_stats(db, SIM_YEAR_WEIGHTS, NUM_SIM_GAMES)
        
        if sim_stats:
            calculate_impact_stats(sim_stats)
            
            sim_scores = calculate_normalized_scores(
                sim_stats, final_benchmarks, target_std_dev, impact_means, impact_std_devs
            )
            
            values_sim = []
            for player_id, scores in sim_scores.items():
                values_sim.append({
                    'player_id': player_id,
                    'season': SIMULATED_SEASON_ID, # <--- Stored as 1
                    **scores
                })
            
            upsert_season_values(db, values_sim, SIMULATED_SEASON_ID)
        else:
            print("No simulated stats generated.")
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        db.close()
        end_time = time.time()
        print(f"\n--- Script finished in {end_time - start_time:.2f} seconds. ---")

if __name__ == "__main__":
    main()