"""
League-Wide Matchup Simulation Script

This script simulates thousands of weekly matchups between every
team in the league to generate win probabilities.

V4:
- ARCHITECTURE CHANGE: Pre-simulates N_SIM_WEEKS for all *players* first.
- Team simulations are now built by summing pre-simulated player-weeks,
  making trade analysis dramatically faster.
- Updated 'find_trades' to use new "sum of wins" logic and
  "win-win" (in both scenarios) criteria.
- Added detailed logging to trade finder.
"""

import random
import time
from typing import Dict, List, Tuple, Optional, Set
from sqlalchemy.orm import Session
from itertools import combinations

# --- Library Imports ---
try:
    from database import SessionLocal, init_db, Player, GameStats
    from teams_and_players_lib import get_league_rosters
    from game_picker_lib import (
        predict_all_player_probabilities,
        generate_weighted_game_samples,
        _create_dummy_game
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
ALL_STAT_KEYS = ['pts', 'reb', 'ast', 'stl', 'blk', 'tpm', 'to', 'fga', 'fgm', 'fta', 'ftm']


# --- Helper Functions ---

def get_all_player_game_pools(
    session: Session,
    player_id_set: Set[str]
) -> Dict[str, List[GameStats]]:
    """
    Generates the N_GAMES_TO_GENERATE-game pool for every player.
    (This is the new Step 1)
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
    
    print(f"Generating {N_GAMES_TO_GENERATE} game samples for each player...")
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


def pre_simulate_player_weeks(
    game_pools: Dict[str, List[GameStats]],
    player_id_set: Set[str],
    num_weeks: int
) -> Dict[str, List[Dict[str, float]]]:
    """
    Pre-simulates N_SIM_WEEKS for every player individually.
    (This is the new Step 2)
    """
    print(f"\n--- Step 2: Pre-simulating {num_weeks} weeks for {len(player_id_set)} players ---")
    start_time = time.time()
    
    player_weekly_stats_map = {player_id: [] for player_id in player_id_set}
    
    for player_id in player_id_set:
        player_pool = game_pools.get(player_id)
        player_weeks = []
        
        if not player_pool:
            # Player has no game pool (e.g., new rookie), create 5000 empty weeks
            dummy_week = {key: 0.0 for key in ALL_STAT_KEYS}
            player_weekly_stats_map[player_id] = [dummy_week] * num_weeks
            continue
            
        for _ in range(num_weeks):
            weekly_totals = {key: 0.0 for key in ALL_STAT_KEYS}
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
            
            player_weeks.append(weekly_totals)
        
        player_weekly_stats_map[player_id] = player_weeks

    end_time = time.time()
    print(f"--- Player-weeks simulated in {end_time - start_time:.2f} seconds ---")
    return player_weekly_stats_map


def build_team_weeks_from_players(
    roster_player_ids: List[str],
    player_weekly_stats_map: Dict[str, List[Dict[str, float]]],
    num_weeks: int
) -> List[Dict[str, float]]:
    """
    Builds a team's N_SIM_WEEKS stats by summing its players' pre-simulated weeks.
    (This is the new, fast team simulation)
    """
    team_weekly_stats_list = []
    for k in range(num_weeks):
        team_week_k_stats = {key: 0.0 for key in ALL_STAT_KEYS}
        
        for player_id in roster_player_ids:
            player_week_k_stats = player_weekly_stats_map.get(player_id, [])
            if player_week_k_stats: # Ensure player exists in map
                for key in ALL_STAT_KEYS:
                    team_week_k_stats[key] += player_week_k_stats[k][key]
                    
        team_weekly_stats_list.append(team_week_k_stats)
        
    return team_weekly_stats_list


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


def compare_n_weeks(
    t1_week_list: List[Dict[str, float]],
    t2_week_list: List[Dict[str, float]]
) -> Dict[str, float]:
    """Compares two lists of pre-simulated weeks and returns win probabilities for Team 1."""
    
    total_wins = {cat: 0.0 for cat in CATEGORIES + ['overall']}
    num_weeks = len(t1_week_list)
    if num_weeks == 0:
        return total_wins

    for k in range(num_weeks):
        t1_totals = t1_week_list[k]
        t2_totals = t2_week_list[k]
        
        t1_weekly_wins = {}
        for cat in ['pts', 'reb', 'ast', 'stl', 'blk', 'tpm']:
            if t1_totals[cat] > t2_totals[cat]: t1_weekly_wins[cat] = 1
            elif t1_totals[cat] < t2_totals[cat]: t1_weekly_wins[cat] = 0
            else: t1_weekly_wins[cat] = 0.5
                
        if t1_totals['to'] < t2_totals['to']: t1_weekly_wins['to'] = 1
        elif t1_totals['to'] > t2_totals['to']: t1_weekly_wins['to'] = 0
        else: t1_weekly_wins['to'] = 0.5
            
        t1_fg_pct = t1_totals['fgm'] / t1_totals['fga'] if t1_totals['fga'] > 0 else 0
        t2_fg_pct = t2_totals['fgm'] / t2_totals['fga'] if t2_totals['fga'] > 0 else 0
        if t1_fg_pct > t2_fg_pct: t1_weekly_wins['fg%'] = 1
        elif t1_fg_pct < t2_fg_pct: t1_weekly_wins['fg%'] = 0
        else: t1_weekly_wins['fg%'] = 0.5
            
        t1_ft_pct = t1_totals['ftm'] / t1_totals['fta'] if t1_totals['fta'] > 0 else 0
        t2_ft_pct = t2_totals['ftm'] / t2_totals['fta'] if t2_totals['fta'] > 0 else 0
        if t1_ft_pct > t2_ft_pct: t1_weekly_wins['ft%'] = 1
        elif t1_ft_pct < t2_ft_pct: t1_weekly_wins['ft%'] = 0
        else: t1_weekly_wins['ft%'] = 0.5
            
        total_cat_wins = sum(t1_weekly_wins.values())
        if total_cat_wins > 4.5: t1_weekly_wins['overall'] = 1
        elif total_cat_wins == 4.5: t1_weekly_wins['overall'] = 0.5
        else: t1_weekly_wins['overall'] = 0
        
        for cat in t1_weekly_wins:
            total_wins[cat] += t1_weekly_wins[cat]
            
    win_probs = {cat: (total_wins[cat] / num_weeks) for cat in total_wins}
    return win_probs


def calculate_average_stats(
    weekly_stats_list: List[Dict[str, float]]
) -> Dict[str, float]:
    """Calculates the average per-week stats from a list of simulated weeks."""
    if not weekly_stats_list:
        return {cat: 0.0 for cat in CATEGORIES + ['fgm', 'fga', 'ftm', 'fta']}
    
    num_weeks = len(weekly_stats_list)
    avg_stats = {}
    
    for cat in ALL_STAT_KEYS:
        avg_stats[cat] = sum(week[cat] for week in weekly_stats_list) / num_weeks
        
    total_fga = sum(week['fga'] for week in weekly_stats_list)
    total_fgm = sum(week['fgm'] for week in weekly_stats_list)
    total_fta = sum(week['fta'] for week in weekly_stats_list)
    total_ftm = sum(week['ftm'] for week in weekly_stats_list)
    
    avg_stats['fg%'] = total_fgm / total_fga if total_fga > 0 else 0.0
    avg_stats['ft%'] = total_ftm / total_fta if total_fta > 0 else 0.0
    
    return avg_stats

# --- CSV Printing Functions (No changes) ---

def print_csv_header():
    header = [
        "team_1", "team_2", "FullStrength", "Win%_Overall",
        "Win%_Points", "Win%_Rebounds", "Win%_Assists", "Win%_Steals",
        "Win%_Turnovers", "Win%_Blocks", "Win%_3_Pointers",
        "Win%_FG_Pct", "Win%_FT_Pct"
    ]
    print(",".join(header))

def print_csv_row(team1, team2, is_full_strength, win_probs):
    row = [
        team1, team2, str(is_full_strength),
        f"{win_probs['overall']:.4f}", f"{win_probs['pts']:.4f}",
        f"{win_probs['reb']:.4f}", f"{win_probs['ast']:.4f}",
        f"{win_probs['stl']:.4f}", f"{win_probs['to']:.4f}",
        f"{win_probs['blk']:.4f}", f"{win_probs['tpm']:.4f}",
        f"{win_probs['fg%']:.4f}", f"{win_probs['ft%']:.4f}"
    ]
    print(",".join(row))

def print_avg_stats_header():
    print("\n--- Average Weekly Stats ---")
    header = [
        "team", "FullStrength", "PTS", "REB", "AST", "STL", "BLK",
        "3PM", "TO", "FGM", "FGA", "FG_Pct", "FTM", "FTA", "FT_Pct"
    ]
    print(",".join(header))
    
def print_avg_stats_row(team, is_full_strength, avg_stats):
    row = [
        team, str(is_full_strength),
        f"{avg_stats['pts']:.2f}", f"{avg_stats['reb']:.2f}",
        f"{avg_stats['ast']:.2f}", f"{avg_stats['stl']:.2f}",
        f"{avg_stats['blk']:.2f}", f"{avg_stats['tpm']:.2f}",
        f"{avg_stats['to']:.2f}", f"{avg_stats['fgm']:.2f}",
        f"{avg_stats['fga']:.2f}", f"{avg_stats['fg%']:.4f}",
        f"{avg_stats['ftm']:.2f}", f"{avg_stats['fta']:.2f}",
        f"{avg_stats['ft%']:.4f}",
    ]
    print(",".join(row))


# --- Main Simulation Function (MODIFIED) ---

def run_league_simulation(
    session: Session,
    rosters_map: Dict,
    player_weekly_stats_map: Dict,
    id_to_name_map: Dict
) -> Tuple[Dict, Dict, Dict]:
    """
    Runs the full H2H simulation and average stats report using
    pre-simulated player-weeks.
    (This is the new Step 3)
    
    Returns:
        all_team_weekly_stats: The raw {team: {scenario: [week_list]}} data
        all_h2h_probs: The raw { (t1, t2, scenario): {win_probs} } data
    """
    print(f"\n--- Step 3: Building teams and running baseline H2H matchups ---")
    
    all_team_weekly_stats = {} 
    team_names = sorted(list(rosters_map.keys()))
    
    for team_name in team_names:
        all_team_weekly_stats[team_name] = {}
        team_full_roster = rosters_map[team_name]
        
        for is_full_strength in [True, False]:
            scenario = "FullStrength" if is_full_strength else "Current"
            active_roster = filter_roster(team_full_roster, is_full_strength)
            
            # --- ROSTER SIZE CHECK ---
            roster_count = len(active_roster)
            if roster_count != EXPECTED_ROSTER_SIZE:
                print("\n--- FATAL ROSTER SIZE ERROR ---")
                print(f"Team: {team_name} | Scenario: {scenario}")
                print(f"ERROR: Roster size is {roster_count}. Expected: {EXPECTED_ROSTER_SIZE}")
                print(f"\nPlayers found on this roster ({roster_count}):")
                if not active_roster: print("  (No players found)")
                for player_id in sorted(active_roster):
                    print(f"  - {id_to_name_map.get(player_id, 'Unknown')} ({player_id})")
                print("\nPlease correct the 'league_rosters.csv' file and try again.")
                session.close()
                exit(1)
            # --- END CHECK ---
            
            # --- MODIFIED: Use new builder function ---
            weekly_stats_list = build_team_weeks_from_players(
                active_roster,
                player_weekly_stats_map,
                N_SIM_WEEKS
            )
            all_team_weekly_stats[team_name][scenario] = weekly_stats_list
    
    print("--- Team building complete, comparing all H2H matchups ---")
    print_csv_header()
    
    all_h2h_probs = {} 
    
    for i in range(len(team_names)):
        for j in range(i + 1, len(team_names)):
            team1_name = team_names[i]
            team2_name = team_names[j]

            for is_full_strength in [True, False]:
                scenario = "FullStrength" if is_full_strength else "Current"
                
                t1_week_list = all_team_weekly_stats[team1_name][scenario]
                t2_week_list = all_team_weekly_stats[team2_name][scenario]
                
                win_probs = compare_n_weeks(t1_week_list, t2_week_list)
                print_csv_row(team1_name, team2_name, is_full_strength, win_probs)
                
                inverse_win_probs = {cat: (1.0 - win_probs[cat]) for cat in win_probs}
                print_csv_row(team2_name, team1_name, is_full_strength, inverse_win_probs)
                
                all_h2h_probs[(team1_name, team2_name, scenario)] = win_probs
                all_h2h_probs[(team2_name, team1_name, scenario)] = inverse_win_probs

    print("\n--- Baseline H2H simulation complete ---")
    
    # --- Print Average Stats ---
    print_avg_stats_header()
    for team_name in team_names:
        for is_full_strength in [True, False]:
            scenario = "FullStrength" if is_full_strength else "Current"
            weekly_stats_list = all_team_weekly_stats[team_name][scenario]
            avg_stats = calculate_average_stats(weekly_stats_list)
            print_avg_stats_row(team_name, is_full_strength, avg_stats)
            
    return all_team_weekly_stats, all_h2h_probs


# --- Trade Finder Function (MODIFIED per new logic) ---

def find_trades(
    n: int,
    team1_name: str,
    team2_name: str,
    rosters_map: Dict,
    player_weekly_stats_map: Dict,
    all_team_weekly_stats: Dict,
    all_h2h_probs: Dict,
    id_to_name_map: Dict
):
    """
    Finds and ranks n-for-n trades where BOTH teams improve their
    SUM of win% against all OTHER teams, in BOTH scenarios.
    (This is the new Step 4)
    """
    print(f"\n--- Step 4: Finding best {n}-for-{n} trades for {team1_name} & {team2_name} ---")
    
    t1_full_roster_tuples = rosters_map[team1_name]
    t2_full_roster_tuples = rosters_map[team2_name]
    
    successful_trades = []
    team_names = sorted(list(rosters_map.keys()))
    other_team_names = [t for t in team_names if t not in [team1_name, team2_name]]
    
    # 1. Calculate the baseline SUM of wins for T1 and T2 against all OTHER teams
    baseline_win_sums = {}
    for scenario in ["FullStrength", "Current"]:
        t1_baseline_sum = sum(all_h2h_probs[(team1_name, other, scenario)]['overall'] for other in other_team_names)
        t2_baseline_sum = sum(all_h2h_probs[(team2_name, other, scenario)]['overall'] for other in other_team_names)
        baseline_win_sums[scenario] = {'t1': t1_baseline_sum, 't2': t2_baseline_sum}
        
    print(f"Baseline Win Sums ({team1_name} vs. rest): FS={baseline_win_sums['FullStrength']['t1']:.2f}, Current={baseline_win_sums['Current']['t1']:.2f}")
    print(f"Baseline Win Sums ({team2_name} vs. rest): FS={baseline_win_sums['FullStrength']['t2']:.2f}, Current={baseline_win_sums['Current']['t2']:.2f}")

    # 2. Get all player combinations
    # (We get combos for the 'FullStrength' roster to consider all players)
    t1_roster_fs = filter_roster(t1_full_roster_tuples, is_full_strength=True)
    t2_roster_fs = filter_roster(t2_full_roster_tuples, is_full_strength=True)

    try:
        t1_combos = list(combinations(t1_roster_fs, n))
        t2_combos = list(combinations(t2_roster_fs, n))
    except ValueError as e:
        print(f"ERROR: Cannot make {n}-player combinations. {e}")
        return
            
    total_trades_to_check = len(t1_combos) * len(t2_combos)
    if total_trades_to_check == 0:
        print("No trades to check.")
        return
        
    print(f"\nChecking {total_trades_to_check} possible {n}-for-{n} trades...")
    checked_count = 0
    
    def get_player_names(player_ids):
        return [id_to_name_map.get(pid, pid) for pid in player_ids]

    # 3. Loop through every trade combination
    for t1_players_out in t1_combos:
        for t2_players_out in t2_combos:
            
            checked_count += 1
            is_win_win_both_scenarios = True
            trade_results_by_scenario = {}
            
            if checked_count % 100 == 0 or total_trades_to_check < 100:
                print(f"\n--- [Trade {checked_count} / {total_trades_to_check}] ---")
                t1_gives_names = ", ".join(get_player_names(t1_players_out))
                t2_gives_names = ", ".join(get_player_names(t2_players_out))
                print(f"Checking: {team1_name} gives [{t1_gives_names}] for [{t2_gives_names}]")

            # 4. Check trade for BOTH scenarios
            for scenario in ["FullStrength", "Current"]:
                
                # Get correct baseline rosters for this scenario
                t1_roster = filter_roster(t1_full_roster_tuples, is_full_strength=(scenario=="FullStrength"))
                t2_roster = filter_roster(t2_full_roster_tuples, is_full_strength=(scenario=="FullStrength"))
                
                # Check if this trade is valid for the current scenario
                # (e.g., an INJ player might not be in the 'Current' roster)
                if not all(p in t1_roster for p in t1_players_out) or \
                   not all(p in t2_roster for p in t2_players_out):
                    is_win_win_both_scenarios = False
                    if checked_count % 100 == 0: print(f"Skipping {scenario}: Player not in active roster.")
                    break # This trade is invalid for this scenario, so it fails the "both" check
                
                # Build new rosters
                t1_new_roster = [p for p in t1_roster if p not in t1_players_out] + list(t2_players_out)
                t2_new_roster = [p for p in t2_roster if p not in t2_players_out] + list(t1_players_out)
                
                # --- FAST SIMULATION ---
                t1_new_weeks = build_team_weeks_from_players(t1_new_roster, player_weekly_stats_map, N_SIM_WEEKS)
                t2_new_weeks = build_team_weeks_from_players(t2_new_roster, player_weekly_stats_map, N_SIM_WEEKS)
                
                # 5. Calculate new win sums against OTHER teams
                t1_new_total_wins = 0.0
                t2_new_total_wins = 0.0
                
                for other_team_name in other_team_names:
                    other_weeks = all_team_weekly_stats[other_team_name][scenario]
                    
                    probs1 = compare_n_weeks(t1_new_weeks, other_weeks)
                    t1_new_total_wins += probs1['overall']
                    
                    probs2 = compare_n_weeks(t2_new_weeks, other_weeks)
                    t2_new_total_wins += probs2['overall']

                # 6. Check if this scenario is a win-win
                t1_baseline_sum = baseline_win_sums[scenario]['t1']
                t2_baseline_sum = baseline_win_sums[scenario]['t2']
                
                t1_gain = t1_new_total_wins - t1_baseline_sum
                t2_gain = t2_new_total_wins - t2_baseline_sum

                if t1_gain <= 0 or t2_gain <= 0:
                    is_win_win_both_scenarios = False
                    if checked_count % 100 == 0:
                        print(f"Failed {scenario}: T1 Gain={t1_gain:.3f}, T2 Gain={t2_gain:.3f}")
                    break # Fails the "win-win" for this scenario
                
                # --- LOGGING: H2H between T1 and T2 ---
                new_h2h_probs = compare_n_weeks(t1_new_weeks, t2_new_weeks)
                if checked_count % 100 == 0 or total_trades_to_check < 100:
                    print(f"  H2H ({scenario}) - {team1_name} Win %: {new_h2h_probs['overall']:.2%}")
                
                trade_results_by_scenario[scenario] = {
                    "t1_gain": t1_gain,
                    "t2_gain": t2_gain,
                    "t1_old_sum": t1_baseline_sum,
                    "t1_new_sum": t1_new_total_wins,
                    "t2_old_sum": t2_baseline_sum,
                    "t2_new_sum": t2_new_total_wins,
                    "new_h2h_probs": new_h2h_probs
                }

            # 7. If it was a win-win in BOTH scenarios, store it
            if is_win_win_both_scenarios:
                print(f"*** SUCCESSFUL TRADE FOUND (Trade {checked_count}) ***")
                combined_gain = (
                    trade_results_by_scenario['FullStrength']['t1_gain'] +
                    trade_results_by_scenario['FullStrength']['t2_gain'] +
                    trade_results_by_scenario['Current']['t1_gain'] +
                    trade_results_by_scenario['Current']['t2_gain']
                )
                
                successful_trades.append({
                    "t1_gives": t1_players_out,
                    "t2_gives": t2_players_out,
                    "combined_gain": combined_gain,
                    "results_fs": trade_results_by_scenario['FullStrength'],
                    "results_current": trade_results_by_scenario['Current']
                })

    print(f"\n--- Trade check complete. {len(successful_trades)} successful trades found. ---")

    # 8. Sort and print final results
    if not successful_trades:
        print(f"No beneficial {n}-for-{n} 'win-win' trades found for {team1_name} & {team2_name}.")
        return
        
    print(f"\n--- Top 'Win-Win' Trades (by Combined Total Gain) for {team1_name} & {team2_name} ---")
    
    sorted_trades = sorted(successful_trades, key=lambda x: x['combined_gain'], reverse=True)
    
    for i, trade in enumerate(sorted_trades[:10]): # Print top 10
        t1_gives_names = ", ".join(get_player_names(trade['t1_gives']))
        t2_gives_names = ", ".join(get_player_names(trade['t2_gives']))
        
        print(f"\n{i+1}. {team1_name} GIVES [{t1_gives_names}] FOR {team2_name}'S [{t2_gives_names}]")
        print(f"   Total Combined Gain: +{trade['combined_gain']:.3f} wins")
        
        print("   --- FullStrength Scenario ---")
        res_fs = trade['results_fs']
        print(f"     {team1_name}: {res_fs['t1_old_sum']:.2f} -> {res_fs['t1_new_sum']:.2f} ({res_fs['t1_gain']:+.3f} win sum)")
        print(f"     {team2_name}: {res_fs['t2_old_sum']:.2f} -> {res_fs['t2_new_sum']:.2f} ({res_fs['t2_gain']:+.3f} win sum)")
        print(f"     New H2H: {team1_name} wins {res_fs['new_h2h_probs']['overall']:.2%}")

        print("   --- Current Scenario ---")
        res_cur = trade['results_current']
        print(f"     {team1_name}: {res_cur['t1_old_sum']:.2f} -> {res_cur['t1_new_sum']:.2f} ({res_cur['t1_gain']:+.3f} win sum)")
        print(f"     {team2_name}: {res_cur['t2_old_sum']:.2f} -> {res_cur['t2_new_sum']:.2f} ({res_cur['t2_gain']:+.3f} win sum)")
        print(f"     New H2H: {team1_name} wins {res_cur['new_h2h_probs']['overall']:.2%}")


# --- Main Execution Block ---

if __name__ == "__main__":
    print("--- Starting Full League Simulation & Trade Analysis (V4) ---")
    main_start_time = time.time()
    
    session = SessionLocal()
    
    try:
        init_db()
        rosters_map = get_league_rosters()
        if not rosters_map:
            raise Exception("Could not get league rosters.")
            
        all_player_ids_set = set(pid for r in rosters_map.values() for pid, s in r)
        
        player_map_query = session.query(Player.player_id, Player.name).all()
        id_to_name_map = {p.player_id: p.name for p in player_map_query}
        
        # --- NEW STEP 1 ---
        game_pools = get_all_player_game_pools(session, all_player_ids_set)
        
        # --- NEW STEP 2 ---
        player_weekly_stats_map = pre_simulate_player_weeks(
            game_pools,
            all_player_ids_set,
            N_SIM_WEEKS
        )
        
    except Exception as e:
        print(f"FATAL ERROR during setup: {e}")
        session.close()
        exit(1)

    # --- NEW STEP 3 ---
    all_team_weekly_stats, all_h2h_probs = run_league_simulation(
        session, rosters_map, player_weekly_stats_map, id_to_name_map
    )
    
    # --- NEW STEP 4 (Example) ---
    find_trades(
        n=2,
        team1_name="Alex",
        team2_name="Drodge",
        rosters_map=rosters_map,
        player_weekly_stats_map=player_weekly_stats_map,
        all_team_weekly_stats=all_team_weekly_stats,
        all_h2h_probs=all_h2h_probs,
        id_to_name_map=id_to_name_map
    )
    
    session.close()
    main_end_time = time.time()
    print(f"\n--- Script Finished. Total time: {main_end_time - main_start_time:.2f} seconds ---")