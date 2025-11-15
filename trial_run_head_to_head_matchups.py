"""
League-Wide Matchup Simulation Script

This script simulates thousands of weekly matchups between every
team in the league to generate win probabilities.

V3:
- Added a debug printout. If a roster count is incorrect,
  it will print the list of players on that roster.
"""

import random
import time
from typing import Dict, List, Tuple, Optional, Set
import numpy as np # For easier averaging
from sqlalchemy.orm import Session

# --- Library Imports ---
try:
    from database import SessionLocal, init_db, Player, GameStats
    from teams_and_players_lib import get_league_rosters
    from game_picker_lib import (
        predict_all_player_probabilities,
        generate_weighted_game_samples,
        _create_dummy_game # We need this helper
    )
except ImportError as e:
    print(f"FATAL ERROR: Could not import libraries: {e}")
    print("Please ensure database.py, teams_and_players_lib.py, and")
    print("game_picker_lib.py are in the same directory.")
    exit(1)


# --- Simulation Configuration ---
N_GAMES_TO_GENERATE = 10000
N_SIM_WEEKS = 5000 
SIM_YEAR_WEIGHTS = [(2026, 1), (2025, 0.2), (2024, 0.1)]
PRIOR_PLAY_PERCENTAGE = 0.85
PRIOR_STRENGTH_IN_GAMES = 82.0
EXPECTED_ROSTER_SIZE = 13
CATEGORIES = ['pts', 'reb', 'ast', 'stl', 'blk', 'tpm', 'to', 'fg%', 'ft%']


# --- Helper Functions ---

def get_all_player_game_pools(
    session: Session,
    player_id_set: Set[str]
) -> Dict[str, List[GameStats]]:
    """
    Generates the N_GAMES_TO_GENERATE-game pool for every player.
    """
    print(f"\n--- Step 1: Generating {N_GAMES_TO_GENERATE}-game pools for {len(player_id_set)} players ---")
    start_time = time.time()
    
    print("Calculating player availability...")
    availability_map = predict_all_player_probabilities(
        session,
        SIM_YEAR_WEIGHTS,
        PRIOR_PLAY_PERCENTAGE,
        PRIOR_STRENGTH_IN_GAMES
    )
    
    print(f"Generating {N_GAMES_TO_GENERATE} game samples for each player (this may take a while)...")
    game_pools = generate_weighted_game_samples(
        session=session,
        player_ids=list(player_id_set),
        num_games=N_GAMES_TO_GENERATE,
        year_weights=SIM_YEAR_WEIGHTS,
        availability_predictions=availability_map,
        include_dummy_games=True 
    )
    
    end_time = time.time()
    print(f"--- Game pools generated in {end_time - start_time:.2f} seconds ---")
    return game_pools


def filter_roster(
    full_roster: List[Tuple[str, Optional[str]]],
    is_full_strength: bool
) -> List[str]:
    """Filters a team's roster based on the simulation scenario."""
    active_roster = []
    for player_id, status in full_roster:
        if is_full_strength:
            if status != 'DROP':
                active_roster.append(player_id)
        else:
            if status != 'INJ':
                active_roster.append(player_id)
    return active_roster


def simulate_week(
    roster_player_ids: List[str],
    game_pools: Dict[str, List[GameStats]]
) -> Dict[str, float]:
    """Simulates one week for a single team."""
    
    weekly_totals = {
        'pts': 0, 'reb': 0, 'ast': 0, 'stl': 0, 'blk': 0,
        'tpm': 0, 'to': 0, 'fga': 0, 'fgm': 0, 'fta': 0, 'ftm': 0
    }
    
    for player_id in roster_player_ids:
        player_pool = game_pools.get(player_id)
        if not player_pool:
            continue
            
        num_games = 3 if random.random() < 0.5 else 4
        
        simulated_games = random.sample(player_pool, num_games)
        
        for game in simulated_games:
            weekly_totals['pts'] += (game.points or 0)
            weekly_totals['reb'] += (game.total_rebounds or 0)
            weekly_totals['ast'] += (game.assists or 0)
            weekly_totals['stl'] += (game.steals or 0)
            weekly_totals['blk'] += (game.blocks or 0)
            weekly_totals['tpm'] += (game.three_pointers or 0)
            weekly_totals['to'] += (game.turnovers or 0)
            
            weekly_totals['fga'] += (game.field_goal_attempts or 0)
            weekly_totals['fgm'] += (game.field_goals or 0)
            weekly_totals['fta'] += (game.free_throw_attempts or 0)
            weekly_totals['ftm'] += (game.free_throws or 0)
            
    return weekly_totals


def calculate_weekly_matchup(
    t1_totals: Dict[str, float],
    t2_totals: Dict[str, float]
) -> Dict[str, float]:
    """Compares two teams' weekly totals and returns the wins for Team 1."""
    t1_wins = {}

    for cat in ['pts', 'reb', 'ast', 'stl', 'blk', 'tpm']:
        if t1_totals[cat] > t2_totals[cat]: t1_wins[cat] = 1
        elif t1_totals[cat] < t2_totals[cat]: t1_wins[cat] = 0
        else: t1_wins[cat] = 0.5
            
    if t1_totals['to'] < t2_totals['to']: t1_wins['to'] = 1
    elif t1_totals['to'] > t2_totals['to']: t1_wins['to'] = 0
    else: t1_wins['to'] = 0.5
        
    t1_fg_pct = t1_totals['fgm'] / t1_totals['fga'] if t1_totals['fga'] > 0 else 0
    t2_fg_pct = t2_totals['fgm'] / t2_totals['fga'] if t2_totals['fga'] > 0 else 0
    if t1_fg_pct > t2_fg_pct: t1_wins['fg%'] = 1
    elif t1_fg_pct < t2_fg_pct: t1_wins['fg%'] = 0
    else: t1_wins['fg%'] = 0.5
        
    t1_ft_pct = t1_totals['ftm'] / t1_totals['fta'] if t1_totals['fta'] > 0 else 0
    t2_ft_pct = t2_totals['ftm'] / t2_totals['fta'] if t2_totals['fta'] > 0 else 0
    if t1_ft_pct > t2_ft_pct: t1_wins['ft%'] = 1
    elif t1_ft_pct < t2_ft_pct: t1_wins['ft%'] = 0
    else: t1_wins['ft%'] = 0.5
        
    total_cat_wins = sum(t1_wins.values())
    if total_cat_wins > 4.5: t1_wins['overall'] = 1
    elif total_cat_wins == 4.5: t1_wins['overall'] = 0.5
    else: t1_wins['overall'] = 0
        
    return t1_wins

def calculate_average_stats(
    weekly_stats_list: List[Dict[str, float]]
) -> Dict[str, float]:
    """Calculates the average per-week stats from a list of simulated weeks."""
    if not weekly_stats_list:
        return {cat: 0.0 for cat in CATEGORIES + ['fgm', 'fga', 'ftm', 'fta']}
    
    num_weeks = len(weekly_stats_list)
    avg_stats = {}
    
    all_stat_keys = ['pts', 'reb', 'ast', 'stl', 'blk', 'tpm', 'to', 'fga', 'fgm', 'fta', 'ftm']
    for cat in all_stat_keys:
        avg_stats[cat] = sum(week[cat] for week in weekly_stats_list) / num_weeks
        
    total_fga = sum(week['fga'] for week in weekly_stats_list)
    total_fgm = sum(week['fgm'] for week in weekly_stats_list)
    total_fta = sum(week['fta'] for week in weekly_stats_list)
    total_ftm = sum(week['ftm'] for week in weekly_stats_list)
    
    avg_stats['fg%'] = total_fgm / total_fga if total_fga > 0 else 0.0
    avg_stats['ft%'] = total_ftm / total_fta if total_fta > 0 else 0.0
    
    return avg_stats

def print_csv_header():
    """Prints the H2H CSV header row."""
    header = [
        "team_1", "team_2", "FullStrength", "Win%_Overall",
        "Win%_Points", "Win%_Rebounds", "Win%_Assists", "Win%_Steals",
        "Win%_Turnovers", "Win%_Blocks", "Win%_3_Pointers",
        "Win%_FG_Pct", "Win%_FT_Pct"
    ]
    print(",".join(header))

def print_csv_row(
    team1: str,
    team2: str,
    is_full_strength: bool,
    win_probs: Dict[str, float]
):
    """Prints a formatted H2H CSV data row."""
    row = [
        team1,
        team2,
        str(is_full_strength),
        f"{win_probs['overall']:.4f}",
        f"{win_probs['pts']:.4f}",
        f"{win_probs['reb']:.4f}",
        f"{win_probs['ast']:.4f}",
        f"{win_probs['stl']:.4f}",
        f"{win_probs['to']:.4f}",
        f"{win_probs['blk']:.4f}",
        f"{win_probs['tpm']:.4f}",
        f"{win_probs['fg%']:.4f}",
        f"{win_probs['ft%']:.4f}"
    ]
    print(",".join(row))

def print_avg_stats_header():
    """Prints the average stats CSV header row."""
    print("\n--- Average Weekly Stats ---")
    header = [
        "team", "FullStrength", "PTS", "REB", "AST", "STL", "BLK",
        "3PM", "TO", "FGM", "FGA", "FG_Pct", "FTM", "FTA", "FT_Pct"
    ]
    print(",".join(header))
    
def print_avg_stats_row(team: str, is_full_strength: bool, avg_stats: Dict[str, float]):
    """Prints a formatted average stats CSV data row."""
    row = [
        team,
        str(is_full_strength),
        f"{avg_stats['pts']:.2f}",
        f"{avg_stats['reb']:.2f}",
        f"{avg_stats['ast']:.2f}",
        f"{avg_stats['stl']:.2f}",
        f"{avg_stats['blk']:.2f}",
        f"{avg_stats['tpm']:.2f}",
        f"{avg_stats['to']:.2f}",
        f"{avg_stats['fgm']:.2f}",
        f"{avg_stats['fga']:.2f}",
        f"{avg_stats['fg%']:.4f}",
        f"{avg_stats['ftm']:.2f}",
        f"{avg_stats['fta']:.2f}",
        f"{avg_stats['ft%']:.4f}",
    ]
    print(",".join(row))


# --- Main Simulation ---

def main():
    """Main executable function."""
    print("--- Starting Full League Simulation ---")
    start_time_all = time.time()
    
    # 0. Setup DB
    try:
        init_db()
        session = SessionLocal()
    except Exception as e:
        print(f"FATAL ERROR: Could not connect to database: {e}")
        exit(1)
    
    # 1. Get League Rosters
    rosters_map = get_league_rosters()
    if not rosters_map:
        print("FATAL ERROR: Could not get league rosters.")
        session.close()
        exit(1)
        
    all_player_ids_set = set()
    for team_roster in rosters_map.values():
        for player_id, status in team_roster:
            all_player_ids_set.add(player_id)
            
    # --- NEW: Build ID-to-Name map for debugging ---
    print("Building player ID-to-Name map for debugging...")
    try:
        player_map_query = session.query(Player.player_id, Player.name).all()
        id_to_name_map = {p.player_id: p.name for p in player_map_query}
    except Exception as e:
        print(f"Warning: Could not build ID-to-Name map. Error: {e}")
        id_to_name_map = {} # Continue without it
    # --- END NEW ---
            
    # 2. Get Game Pools for ALL players
    game_pools = get_all_player_game_pools(session, all_player_ids_set)
    
    # --- Step 3: Pre-simulate all team-weeks ---
    print(f"\n--- Step 2: Pre-simulating {N_SIM_WEEKS} weeks for all teams ---")
    
    all_team_weekly_stats = {} 
    
    team_names = sorted(list(rosters_map.keys()))
    
    for team_name in team_names:
        all_team_weekly_stats[team_name] = {}
        team_full_roster = rosters_map[team_name]
        
        for is_full_strength in [True, False]:
            scenario = "FullStrength" if is_full_strength else "Current"
            active_roster = filter_roster(team_full_roster, is_full_strength)
            
            # --- Roster Count Check (MODIFIED) ---
            roster_count = len(active_roster)
            if roster_count != EXPECTED_ROSTER_SIZE:
                print("\n--- FATAL ROSTER SIZE ERROR ---")
                print(f"Team: {team_name}")
                print(f"Scenario: {scenario}")
                print(f"ERROR: Roster size is {roster_count}. Expected: {EXPECTED_ROSTER_SIZE}")
                
                # --- NEW PRINT LOGIC ---
                print(f"\nPlayers found on this roster ({roster_count}):")
                if not active_roster:
                    print("  (No players found)")
                for player_id in sorted(active_roster):
                    player_name = id_to_name_map.get(player_id, "Unknown Name")
                    print(f"  - {player_name} ({player_id})")
                # --- END NEW LOGIC ---
                    
                print("\nPlease correct the 'league_rosters.csv' file and try again.")
                session.close()
                exit(1)
            # --- End Check ---
            
            weekly_stats_list = []
            for _ in range(N_SIM_WEEKS):
                weekly_stats_list.append(simulate_week(active_roster, game_pools))
            
            all_team_weekly_stats[team_name][scenario] = weekly_stats_list
    
    print("--- Pre-simulation complete ---")

    # --- Step 4: Run Matchup Comparisons ---
    print(f"\n--- Step 3: Comparing matchups (Head-to-Head Results) ---")
    print_csv_header()
    
    for i in range(len(team_names)):
        for j in range(i + 1, len(team_names)):
            team1_name = team_names[i]
            team2_name = team_names[j]

            for is_full_strength in [True, False]:
                scenario = "FullStrength" if is_full_strength else "Current"
                
                t1_week_list = all_team_weekly_stats[team1_name][scenario]
                t2_week_list = all_team_weekly_stats[team2_name][scenario]
                
                total_wins = {cat: 0.0 for cat in CATEGORIES + ['overall']}
                
                for k in range(N_SIM_WEEKS):
                    t1_totals = t1_week_list[k]
                    t2_totals = t2_week_list[k]
                    
                    weekly_wins = calculate_weekly_matchup(t1_totals, t2_totals)
                    
                    for cat in weekly_wins:
                        total_wins[cat] += weekly_wins[cat]
                
                win_probs = {
                    cat: (total_wins[cat] / N_SIM_WEEKS)
                    for cat in total_wins
                }
                
                # 1. Print A vs B
                print_csv_row(team1_name, team2_name, is_full_strength, win_probs)
                
                # 2. Print B vs A
                inverse_win_probs = {
                    cat: (1.0 - win_probs[cat])
                    for cat in win_probs
                }
                print_csv_row(team2_name, team1_name, is_full_strength, inverse_win_probs)

    # --- Step 5: Print Average Stats ---
    print_avg_stats_header()
    
    for team_name in team_names:
        for is_full_strength in [True, False]:
            scenario = "FullStrength" if is_full_strength else "Current"
            
            weekly_stats_list = all_team_weekly_stats[team_name][scenario]
            avg_stats = calculate_average_stats(weekly_stats_list)
            print_avg_stats_row(team_name, is_full_strength, avg_stats)
    
    # --- Cleanup ---
    session.close()
    end_time_all = time.time()
    print(f"\n--- Simulation Complete. Total time: {end_time_all - start_time_all:.2f} seconds ---")


if __name__ == "__main__":
    main()