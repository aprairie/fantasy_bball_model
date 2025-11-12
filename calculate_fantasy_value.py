import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import func, select, exc
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime
import time

# Import models from database.py
from database import (
    SessionLocal,
    Player,
    EloStats,
    GameStats,
    PlayerSeasonValue
)

# --- SCRIPT CONFIGURATION ---
# The season to use for calculating the averages (a full, complete season)
BENCHMARK_SEASON = 2025 
# The season to apply the averages to (the new/current season)
CALCULATION_SEASON = 2026

MIN_GAMES_PLAYED = 1 
FG_BENCHMARK = 0.48
FT_BENCHMARK = 0.81
# The number of players to use for the "benchmark" cohort
TOP_N_PLAYERS = 200

def get_season_stats(db: Session, season: int):
    """
    Fetches and aggregates all player stats for the given season.
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

def calculate_impact_stats(all_player_stats: dict):
    """
    Calculates and adds the FG% and FT% "impact" stats to each player's dict.
    """
    for player_id, stats in all_player_stats.items():
        if stats['total_fga'] > 0:
            fg_impact = (stats['total_fgm'] - (stats['total_fga'] * FG_BENCHMARK)) / stats['games_played']
        else:
            fg_impact = 0.0
            
        if stats['total_fta'] > 0:
            ft_impact = (stats['total_ftm'] - (stats['total_fta'] * FT_BENCHMARK)) / stats['games_played']
        else:
            ft_impact = 0.0
            
        stats['fg_impact'] = fg_impact
        stats['ft_impact'] = ft_impact

def get_benchmarks(player_stats_cohort: list):
    """
    Calculates the average (mean) for each stat category from a given list
    of player stats.
    """
    # np.finfo(float).eps is a tiny number to prevent divide-by-zero errors
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
            'play_likelihood': stats['games_played'] / 82.0
        }
    return final_scores

def upsert_season_values(db: Session, values_to_upsert: list):
    """
    Efficiently inserts or updates all PlayerSeasonValue records.
    """
    if not values_to_upsert:
        print("No values to upsert.")
        return
        
    print(f"Upserting {len(values_to_upsert)} records for season {CALCULATION_SEASON}...")
    
    table = PlayerSeasonValue.__table__
    stmt = pg_insert(table).values(values_to_upsert)
    
    update_columns = {
        col.name: col
        for col in stmt.excluded
        if not col.primary_key
    }
    
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=['player_id', 'season'],
        set_=update_columns
    )
    
    try:
        db.execute(upsert_stmt)
        db.commit()
        print(f"Successfully upserted {len(values_to_upsert)} records.")
    except exc.SQLAlchemyError as e:
        print(f"[!!!] Database upsert failed: {e}")
        db.rollback()
        print("--- Rolled back changes. ---")

def main():
    print(f"--- Starting Player Season Value Calculation ---")
    print(f"Benchmark Season: {BENCHMARK_SEASON} (for averages)")
    print(f"Calculation Season: {CALCULATION_SEASON} (for final scores)")
    start_time = time.time()
    db: Session = SessionLocal()
    
    try:
        # --- PASS 1: Calculate Benchmarks from BENCHMARK_SEASON ---
        print(f"\n--- Pass 1: Analyzing Benchmark Season {BENCHMARK_SEASON} ---")
        
        # 1. Get all stats for the benchmark season
        benchmark_stats_all = get_season_stats(db, BENCHMARK_SEASON)
        if not benchmark_stats_all:
            print(f"No stats found for benchmark season {BENCHMARK_SEASON}. Exiting.")
            return
            
        # 2. Calculate impact stats for all players
        calculate_impact_stats(benchmark_stats_all)
        
        # 3. Get provisional benchmarks using ALL players to find the top 200
        provisional_benchmarks = get_benchmarks(list(benchmark_stats_all.values()))
        
        # 4. Calculate provisional scores
        # We use placeholder std dev (1.0) and means (0) for this first pass
        # The goal is just to rank players, not get final scores
        provisional_std_devs = {'fg': 1.0, 'ft': 1.0}
        provisional_means = {'fg': 0.0, 'ft': 0.0}
        
        provisional_scores = calculate_normalized_scores(
            benchmark_stats_all, 
            provisional_benchmarks, 
            1.0, 
            provisional_means,
            provisional_std_devs
        )
        
        # 5. Sort by provisional total_score to find the Top N
        sorted_players = sorted(
            provisional_scores.items(), 
            key=lambda item: item[1]['total_score'], 
            reverse=True
        )
        
        top_n_player_ids = {pid for pid, score in sorted_players[:TOP_N_PLAYERS]}
        
        # 6. Create the final cohort of stats from only these Top N players
        final_benchmark_cohort_stats = [
            stats for pid, stats in benchmark_stats_all.items() 
            if pid in top_n_player_ids
        ]
        
        if not final_benchmark_cohort_stats:
            print("Error: Could not create final benchmark cohort. Exiting.")
            return

        print(f"Identified Top {len(final_benchmark_cohort_stats)} players from {BENCHMARK_SEASON} by value.")

        # 7. Calculate FINAL benchmarks using this Top N cohort
        print("Calculating final benchmarks from Top N cohort...")
        final_benchmarks = get_benchmarks(final_benchmark_cohort_stats)
        
        print("--- Final Benchmark Averages (Top 200) ---")
        for k, v in final_benchmarks.items():
            print(f"{k.upper()}: {v:.4f}")
        print("------------------------------------------")

        # 8. Calculate variance targets based on this Top N cohort
        # This is the "target standard deviation" for FG% and FT%
        pts_scores_from_cohort = [
            (s['avg_pts'] / final_benchmarks['pts']) * 100 
            for s in final_benchmark_cohort_stats
        ]
        target_std_dev = np.std(pts_scores_from_cohort)
        
        # We also need the mean and std_dev of the impact stats FROM THE COHORT
        # to use for Z-scoring
        fg_impacts_from_cohort = [s['fg_impact'] for s in final_benchmark_cohort_stats]
        ft_impacts_from_cohort = [s['ft_impact'] for s in final_benchmark_cohort_stats]
        
        impact_means = {
            'fg': np.mean(fg_impacts_from_cohort),
            'ft': np.mean(ft_impacts_from_cohort)
        }
        impact_std_devs = {
            'fg': np.std(fg_impacts_from_cohort) + np.finfo(float).eps,
            'ft': np.std(ft_impacts_from_cohort) + np.finfo(float).eps
        }
        
        print(f"Target variance (std dev of PTS_SCORE): {target_std_dev:.4f}")
        print("--- Pass 1 Complete ---")
        
        # --- PASS 2: Calculate Scores for CALCULATION_SEASON ---
        print(f"\n--- Pass 2: Calculating Scores for {CALCULATION_SEASON} ---")
        
        # 1. Get stats for the calculation season
        calculation_stats = get_season_stats(db, CALCULATION_SEASON)
        if not calculation_stats:
            print(f"No stats found for calculation season {CALCULATION_SEASON}. Exiting.")
            return

        # 2. Calculate impact stats
        calculate_impact_stats(calculation_stats)
        
        # 3. Calculate final scores using benchmarks from Pass 1
        print("Calculating final normalized scores...")
        final_scores_dict = calculate_normalized_scores(
            calculation_stats,
            final_benchmarks,
            target_std_dev,
            impact_means,
            impact_std_devs
        )
        
        # 4. Format for database upsert
        values_to_upsert = []
        for player_id, scores in final_scores_dict.items():
            db_row = {
                'player_id': player_id,
                'season': CALCULATION_SEASON,
                **scores  # Unpack all the calculated score key/value pairs
            }
            values_to_upsert.append(db_row)
        
        # 5. Upsert data into the database
        upsert_season_values(db, values_to_upsert)
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        db.close()
        end_time = time.time()
        print(f"\n--- Script finished in {end_time - start_time:.2f} seconds. ---")

if __name__ == "__main__":
    main()