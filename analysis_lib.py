import time
import random
import sys  # Added for sys.exit()
from typing import Dict, List, Tuple, Optional, Set
from itertools import combinations
from sqlalchemy import desc

# --- Constants ---
EXPECTED_ROSTER_SIZE = 13
CATEGORIES = ['pts', 'reb', 'ast', 'stl', 'blk', 'tpm', 'to', 'fg%', 'ft%']
ALL_STAT_KEYS = ['pts', 'reb', 'ast', 'stl', 'blk', 'tpm', 'to', 'fga', 'fgm', 'fta', 'ftm']

# --- Helper Functions ---

def validate_and_crash_if_invalid_roster(
    team_name: str, 
    roster_ids: List[str], 
    scenario: str, 
    id_to_name_map: Dict
):
    """
    Checks if the roster size matches EXPECTED_ROSTER_SIZE.
    If not, prints detailed debug info and kills the program.
    """
    # We ignore the FreeAgents pool as that can be any size
    if team_name == "FreeAgents":
        return

    if len(roster_ids) != EXPECTED_ROSTER_SIZE:
        print(f"\n{'!'*40}")
        print(f"FATAL ERROR: ROSTER SIZE MISMATCH")
        print(f"{'!'*40}")
        print(f"Team:     {team_name}")
        print(f"Scenario: {scenario}")
        print(f"Expected: {EXPECTED_ROSTER_SIZE}")
        print(f"Actual:   {len(roster_ids)}")
        print(f"{'-'*40}")
        print(f"Current Roster List:")
        
        for i, pid in enumerate(roster_ids, 1):
            p_name = id_to_name_map.get(pid, f"Unknown ID: {pid}")
            print(f"{i:2}. {p_name} ({pid})")
            
        print(f"{'!'*40}")
        print("Exiting immediately due to invalid roster construction.")
        sys.exit(1)

def build_team_weeks_from_players(
    roster_player_ids: List[str],
    player_weekly_stats_map: Dict[str, List[Dict[str, float]]],
    num_weeks: int
) -> List[Dict[str, float]]:
    """
    Builds a team's stats by summing its players' pre-simulated weeks.
    """
    team_weekly_stats_list = []
    for k in range(num_weeks):
        team_week_k_stats = {key: 0.0 for key in ALL_STAT_KEYS}
        
        for player_id in roster_player_ids:
            player_week_k_stats = player_weekly_stats_map.get(player_id, [])
            if player_week_k_stats: 
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
            # In Full Strength, we keep INJ players, but remove DROP players
            if status != 'DROP':
                active_roster.append(player_id)
        else:
            # In Current, we keep DROP players (usually), but remove INJ players
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
        # Counting categories
        for cat in ['pts', 'reb', 'ast', 'stl', 'blk', 'tpm']:
            if t1_totals[cat] > t2_totals[cat]: t1_weekly_wins[cat] = 1
            elif t1_totals[cat] < t2_totals[cat]: t1_weekly_wins[cat] = 0
            else: t1_weekly_wins[cat] = 0.5
        
        # Turnovers (Lower is better)
        if t1_totals['to'] < t2_totals['to']: t1_weekly_wins['to'] = 1
        elif t1_totals['to'] > t2_totals['to']: t1_weekly_wins['to'] = 0
        else: t1_weekly_wins['to'] = 0.5
            
        # Percentages
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

def calculate_average_stats(weekly_stats_list: List[Dict[str, float]]) -> Dict[str, float]:
    """Calculates the average per-week stats."""
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

# --- Printing Helpers ---

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


# --- Core Logic: League Simulation ---

def run_league_simulation(
    rosters_map: Dict,
    player_weekly_stats_map: Dict,
    id_to_name_map: Dict,
    n_sim_weeks: int
) -> Tuple[Dict, Dict]:
    """
    Runs the full H2H simulation and average stats report.
    """
    print(f"\n--- Step 3: Building teams and running baseline H2H matchups ---")
    
    all_team_weekly_stats = {} 
    team_names = sorted(list(rosters_map.keys()))
    
    # Build Team Data
    for team_name in team_names:
        all_team_weekly_stats[team_name] = {}
        team_full_roster = rosters_map[team_name]
        
        for is_full_strength in [True, False]:
            scenario = "FullStrength" if is_full_strength else "Current"
            active_roster = filter_roster(team_full_roster, is_full_strength)
            
            # --- VALIDATION INJECTION ---
            validate_and_crash_if_invalid_roster(team_name, active_roster, scenario, id_to_name_map)
            # ----------------------------

            weekly_stats_list = build_team_weeks_from_players(
                active_roster,
                player_weekly_stats_map,
                n_sim_weeks
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
    
    # Print Average Stats
    print_avg_stats_header()
    for team_name in team_names:
        for is_full_strength in [True, False]:
            scenario = "FullStrength" if is_full_strength else "Current"
            weekly_stats_list = all_team_weekly_stats[team_name][scenario]
            avg_stats = calculate_average_stats(weekly_stats_list)
            print_avg_stats_row(team_name, is_full_strength, avg_stats)
            
    return all_team_weekly_stats, all_h2h_probs

# --- Core Logic: Trade Finder ---

def find_trades(
    n: int,
    team1_name: str,
    team2_name: str,
    rosters_map: dict,
    player_weekly_stats_map: dict,
    all_team_weekly_stats: dict,
    all_h2h_probs: dict,
    id_to_name_map: dict,
    team2_loss_tolerance: float = 0.0,
    allow_trading_injured: bool = True,
    required_players: list[str] = None, 
    n_sim_weeks: int = 5000
):
    """
    Finds trades where Team 1 improves.
    """
    
    # --- IMPORTS FOR FREE AGENT LOGIC ---
    try:
        from database import SessionLocal, PlayerSeasonValue, Player # Added Player import
        from teams_and_players_lib import get_available_free_agents
        # --- NEW IMPORTS FOR ON-THE-FLY SIMULATION ---
        from game_picker_lib import (
            predict_all_player_probabilities,
            generate_weighted_game_samples
        )
    except ImportError:
        print("FATAL ERROR: Missing database.py, teams_and_players_lib.py or game_picker_lib.py for trade finder.")
        return

    print(f"\n--- Step 4: Finding best {n}-for-{n} trades for {team1_name} & {team2_name} ---")
    
    t1_full_roster_tuples = rosters_map[team1_name]
    
    # --- FREE AGENT HANDLING ---
    if team2_name == "FreeAgents":
        print("Targeting FREE AGENTS. Fetching top 30 available players by Z-Score...")
        session = SessionLocal()
        try:
            # 1. Get IDs (Strings)
            fa_ids = get_available_free_agents()
            
            # 2. Rank by total_score (Year 1 / 2025)
            top_fa_query = (
                session.query(Player.player_id, PlayerSeasonValue.total_score)
                .join(PlayerSeasonValue, Player.id == PlayerSeasonValue.player_id)
                .filter(PlayerSeasonValue.season == 2025)
                .filter(Player.player_id.in_(fa_ids))
                .order_by(desc(PlayerSeasonValue.total_score))
                .limit(30)
                .all()
            )
            
            top_fa_ids = [r.player_id for r in top_fa_query]
            print(f"Top Free Agents found: {len(top_fa_ids)}")
            ids = [r.player_id for r in top_fa_query]
            
            # --- NEW STEP 2.5: HYDRATE FREE AGENT STATS ---
            # Check if these players are missing from player_weekly_stats_map
            missing_stats_ids = [pid for pid in top_fa_ids if pid not in player_weekly_stats_map or not player_weekly_stats_map[pid]]
            
            if missing_stats_ids:
                print(f"Generating stats for {len(missing_stats_ids)} free agents...")
                
                # Constants used in main.py
                SIM_YEAR_WEIGHTS = [(2026, 1.2), (2025, 0.2), (2024, 0.1)] 
                AVAILABILITY_SIM_YEAR_WEIGHTS = [(2026, 1), (2025, 1), (2024, 1)]
                PRIOR_PLAY_PERCENTAGE = 0.85
                PRIOR_STRENGTH_IN_GAMES = 82.0
                
                # 1. Predict Availability
                availability_map = predict_all_player_probabilities(
                    session,
                    AVAILABILITY_SIM_YEAR_WEIGHTS,
                    PRIOR_PLAY_PERCENTAGE,
                    PRIOR_STRENGTH_IN_GAMES
                )
                
                # 2. Generate Game Samples
                game_pools = generate_weighted_game_samples(
                    session=session,
                    player_ids=missing_stats_ids,
                    num_games=1000, 
                    year_weights=SIM_YEAR_WEIGHTS,
                    availability_predictions=availability_map,
                    include_dummy_games=True 
                )
                
                # 3. Simulate Weeks
                for pid in missing_stats_ids:
                    player_pool = game_pools.get(pid)
                    player_weeks = []
                    
                    if not player_pool:
                        dummy_week = {key: 0.0 for key in ALL_STAT_KEYS}
                        player_weekly_stats_map[pid] = [dummy_week] * n_sim_weeks
                        continue
                        
                    for _ in range(n_sim_weeks):
                        weekly_totals = {key: 0.0 for key in ALL_STAT_KEYS}
                        # Simple 3 or 4 game logic from main.py
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
                    
                    player_weekly_stats_map[pid] = player_weeks
                
                print("Free Agent stats generated successfully.")

            # 3. Create dummy roster (Assume 'Active' status for FAs)
            t2_full_roster_tuples = [(pid, 'Active') for pid in top_fa_ids]
            
            # 4. Inject dummy stats so simulation doesn't crash when looking up 'FreeAgents'
            all_team_weekly_stats["FreeAgents"] = {
                "FullStrength": [],
                "Current": []
            }
            
        except Exception as e:
            print(f"Error fetching free agents: {e}")
            import traceback
            traceback.print_exc()
            session.close()
            return
        finally:
            session.close()
    else:
        t2_full_roster_tuples = rosters_map[team2_name]
        print(f"Config: T2 Tolerance = -{team2_loss_tolerance} | Allow Injured Trades = {allow_trading_injured}")

    if required_players:
        print(f"Required Players in trade: {[id_to_name_map.get(p, p) for p in required_players]}")

    # 0. Build a Status Map for O(1) lookup
    player_status_map = {}
    for pid, status in t1_full_roster_tuples:
        player_status_map[pid] = status
    for pid, status in t2_full_roster_tuples:
        player_status_map[pid] = status
    
    successful_trades = []
    team_names = sorted(list(rosters_map.keys()))
    # Exclude involved teams from opponents list
    other_team_names = [t for t in team_names if t not in [team1_name, team2_name] and t != "FreeAgents"]
    
    scenarios_to_check = ["FullStrength", "Current"]

    # 1. Calculate the baseline stats
    baseline_data = {s: {team1_name: {}, team2_name: {}} for s in scenarios_to_check}
    
    for scenario in scenarios_to_check:
        # A. Baselines against the rest of the league
        for other in other_team_names:
            # Team 1 Baseline
            if (team1_name, other, scenario) in all_h2h_probs:
                baseline_data[scenario][team1_name][other] = all_h2h_probs[(team1_name, other, scenario)]['overall']
            
            # Team 2 Baseline (Skip if FreeAgents)
            if team2_name != "FreeAgents":
                if (team2_name, other, scenario) in all_h2h_probs:
                    baseline_data[scenario][team2_name][other] = all_h2h_probs[(team2_name, other, scenario)]['overall']
        
        # B. Baseline between Team 1 and Team 2 (The specific match up)
        if team2_name != "FreeAgents":
             if (team1_name, team2_name, scenario) in all_h2h_probs:
                 base_t1_vs_t2 = all_h2h_probs[(team1_name, team2_name, scenario)]['overall']
                 baseline_data[scenario][team1_name][team2_name] = base_t1_vs_t2
                 # t2 vs t1 is just the inverse of t1 vs t2 generally, but let's fetch strictly from lookup if available
                 # or calculate inverse
                 if (team2_name, team1_name, scenario) in all_h2h_probs:
                     baseline_data[scenario][team2_name][team1_name] = all_h2h_probs[(team2_name, team1_name, scenario)]['overall']
                 else:
                     baseline_data[scenario][team2_name][team1_name] = 1.0 - base_t1_vs_t2


    # 2. Identify TRADABLE players
    def get_tradable_players(roster_tuples):
        tradable = []
        for pid, status in roster_tuples:
            if team2_name != "FreeAgents":
                # Only enforce injury restrictions for real teams
                if not allow_trading_injured and status == 'INJ': 
                    continue
            tradable.append(pid)
        return tradable

    t1_tradable_ids = get_tradable_players(t1_full_roster_tuples)
    t2_tradable_ids = get_tradable_players(t2_full_roster_tuples)

    # 3. Generate combinations
    try:
        t1_combos = list(combinations(t1_tradable_ids, n))
        t2_combos = list(combinations(t2_tradable_ids, n))
    except ValueError as e:
        print(f"ERROR: Cannot make {n}-player combinations. {e}")
        return
            
    total_trades_to_check = len(t1_combos) * len(t2_combos)
    print(f"\nChecking {total_trades_to_check} possible {n}-for-{n} trades...")
    checked_count = 0
    
    def get_player_names(player_ids):
        return [id_to_name_map.get(pid, pid) for pid in player_ids]

    # 4. Loop through trade combinations
    for t1_players_out in t1_combos:
        for t2_players_out in t2_combos:
            
            checked_count += 1
            if checked_count % 5000 == 0:
                 print(f" ... checked {checked_count}/{total_trades_to_check}")

            # --- CHECK 1: Required Players ---
            if required_players:
                involved_players = set(t1_players_out) | set(t2_players_out)
                if not set(required_players).issubset(involved_players):
                    continue

            # --- CHECK 2: Status Symmetry (CRITICAL FOR ROSTER SIZE) ---
            if team2_name != "FreeAgents":
                # Real Team Trade: MUST exchange same status types to keep math valid
                t1_drops = sum(1 for p in t1_players_out if player_status_map.get(p) == 'DROP')
                t2_drops = sum(1 for p in t2_players_out if player_status_map.get(p) == 'DROP')
                
                if t1_drops != t2_drops:
                    # Roster size would break in FullStrength scenario
                    continue 

                t1_inj = sum(1 for p in t1_players_out if player_status_map.get(p) == 'INJ')
                t2_inj = sum(1 for p in t2_players_out if player_status_map.get(p) == 'INJ')
                
                if t1_inj != t2_inj:
                    # Roster size would break in Current scenario
                    continue
            else:
                # Free Agent Trade
                # If we drop a 'DROP' player to pick up a FA, we handle it via reassignment logic later.
                # However, if we drop an 'INJ' player to pick up a 'Healthy' FA, the "Current" roster size will increase by 1.
                # We must prevent trading away INJ players for FAs.
                t1_inj = sum(1 for p in t1_players_out if player_status_map.get(p) == 'INJ')
                if t1_inj > 0:
                    continue

            
            # --- Check 2.5: Count Drops leaving T1 (For Free Agent Logic) ---
            drops_exiting_t1 = 0
            if team2_name == "FreeAgents":
                drops_exiting_t1 = sum(1 for p in t1_players_out if player_status_map.get(p) == 'DROP')

            # --- Simulation ---
            is_trade_valid = True
            trade_results_by_scenario = {}

            for scenario in scenarios_to_check:
                # 1. Get current active rosters for this scenario
                is_fs = (scenario == "FullStrength")
                t1_active_base = filter_roster(t1_full_roster_tuples, is_full_strength=is_fs)
                # Team 2 simulation is only needed if it's a real team
                t2_active_base = []
                if team2_name != "FreeAgents":
                    t2_active_base = filter_roster(t2_full_roster_tuples, is_full_strength=is_fs)
                
                # 2. Construct new active rosters
                t1_new_roster = [p for p in t1_active_base if p not in t1_players_out]
                t2_new_roster = []
                if team2_name != "FreeAgents":
                    t2_new_roster = [p for p in t2_active_base if p not in t2_players_out]
                
                # Add players entering
                temp_drops_to_assign = drops_exiting_t1 
                for p in t2_players_out:
                    status = player_status_map.get(p, 'Active') # Default to Active for FAs
                    
                    # --- FIX START ---
                    # If we are dropping a player marked 'DROP' to pick up a Free Agent,
                    # we must mark the Free Agent as 'DROP' for this simulation iteration.
                    if team2_name == "FreeAgents" and temp_drops_to_assign > 0:
                        status = 'DROP'
                        temp_drops_to_assign -= 1
                    # --- FIX END ---
                    
                    if is_fs:
                        if status != 'DROP': t1_new_roster.append(p)
                    else:
                        if status != 'INJ': t1_new_roster.append(p)

                if team2_name != "FreeAgents":
                    for p in t1_players_out:
                        status = player_status_map.get(p)
                        if is_fs:
                            if status != 'DROP': t2_new_roster.append(p)
                        else:
                            if status != 'INJ': t2_new_roster.append(p)
                
                # --- VALIDATION INJECTION ---
                validate_and_crash_if_invalid_roster(team1_name, t1_new_roster, scenario, id_to_name_map)
                if team2_name != "FreeAgents":
                    validate_and_crash_if_invalid_roster(team2_name, t2_new_roster, scenario, id_to_name_map)
                # ----------------------------

                # Fast Sim
                t1_new_weeks = build_team_weeks_from_players(t1_new_roster, player_weekly_stats_map, n_sim_weeks)
                t2_new_weeks = []
                if team2_name != "FreeAgents":
                    t2_new_weeks = build_team_weeks_from_players(t2_new_roster, player_weekly_stats_map, n_sim_weeks)
                
                # Stats containers
                t1_deltas = {}
                t2_deltas = {}
                t1_new_vals = {}
                t2_new_vals = {} 
                
                t1_gain_sum = 0.0
                t2_gain_sum = 0.0
                
                # A. Compare against Rest of League
                for other in other_team_names:
                    other_weeks = all_team_weekly_stats[other][scenario]
                    
                    # Calc Team 1
                    new_win_pct_1 = compare_n_weeks(t1_new_weeks, other_weeks)['overall']
                    base_1 = baseline_data[scenario][team1_name].get(other, 0.5) # Default if missing
                    d1 = new_win_pct_1 - base_1
                    t1_deltas[other] = d1
                    t1_new_vals[other] = new_win_pct_1
                    t1_gain_sum += d1
                    
                    # Calc Team 2 (Only if real team)
                    if team2_name != "FreeAgents":
                        new_win_pct_2 = compare_n_weeks(t2_new_weeks, other_weeks)['overall']
                        base_2 = baseline_data[scenario][team2_name].get(other, 0.5)
                        d2 = new_win_pct_2 - base_2
                        t2_deltas[other] = d2
                        t2_new_vals[other] = new_win_pct_2
                        t2_gain_sum += d2
                    else:
                        t2_deltas[other] = 0.0
                        t2_new_vals[other] = 0.0
                
                # B. Compare Team 1 vs Team 2 (Post Trade)
                if team2_name != "FreeAgents":
                    # Calc Team 1 vs Team 2
                    new_win_pct_1_vs_2 = compare_n_weeks(t1_new_weeks, t2_new_weeks)['overall']
                    base_1_vs_2 = baseline_data[scenario][team1_name].get(team2_name, 0.5)
                    d1_vs_2 = new_win_pct_1_vs_2 - base_1_vs_2
                    
                    t1_deltas[team2_name] = d1_vs_2
                    t1_new_vals[team2_name] = new_win_pct_1_vs_2
                    t1_gain_sum += d1_vs_2
                    
                    # Calc Team 2 vs Team 1 (Inverse)
                    new_win_pct_2_vs_1 = 1.0 - new_win_pct_1_vs_2
                    base_2_vs_1 = baseline_data[scenario][team2_name].get(team1_name, 0.5)
                    d2_vs_1 = new_win_pct_2_vs_1 - base_2_vs_1
                    
                    t2_deltas[team1_name] = d2_vs_1
                    t2_new_vals[team1_name] = new_win_pct_2_vs_1
                    t2_gain_sum += d2_vs_1

                # Check Criteria
                if t1_gain_sum <= 0:
                    is_trade_valid = False
                    break
                
                if team2_name != "FreeAgents":
                    if t2_gain_sum < -team2_loss_tolerance:
                        is_trade_valid = False
                        break 
                
                trade_results_by_scenario[scenario] = {
                    "t1_gain_sum": t1_gain_sum,
                    "t2_gain_sum": t2_gain_sum,
                    "t1_deltas": t1_deltas,
                    "t2_deltas": t2_deltas,
                    "t1_new_vals": t1_new_vals,
                    "t2_new_vals": t2_new_vals
                }

            if is_trade_valid:
                combined_gain = 0
                for scen in scenarios_to_check:
                    combined_gain += trade_results_by_scenario[scen]['t1_gain_sum']
                    # Only add t2 gain if it's a real trade, otherwise we just maximize t1
                    if team2_name != "FreeAgents":
                        combined_gain += trade_results_by_scenario[scen]['t2_gain_sum']
                
                successful_trades.append({
                    "t1_gives": t1_players_out,
                    "t2_gives": t2_players_out,
                    "combined_gain": combined_gain,
                    "results": trade_results_by_scenario
                })

    print(f"\n--- Trade check complete. {len(successful_trades)} successful trades found. ---")

    if not successful_trades:
        print(f"No trades found matching criteria.")
        return
        
    print(f"\n--- Top Trades (Sorted by Combined Gain) ---")
    sorted_trades = sorted(successful_trades, key=lambda x: x['combined_gain'], reverse=True)
    
    for i, trade in enumerate(sorted_trades[:15]):
        t1_names = ", ".join(get_player_names(trade['t1_gives']))
        t2_names = ", ".join(get_player_names(trade['t2_gives']))
        
        print(f"\n{'='*85}")
        print(f"TRADE #{i+1}: {team1_name} gets [{t2_names}] <--> {team2_name} gets [{t1_names}]")
        print(f"Combined Metric: {trade['combined_gain']:.4f}")
        print(f"{'-'*85}")

        def print_team_table(team_name, is_team_1):
            # If it's FreeAgents, skip printing the table (it's empty/meaningless)
            if team_name == "FreeAgents":
                return

            res_curr = trade['results']['Current']
            res_fs   = trade['results']['FullStrength']
            
            key_delta = 't1_deltas' if is_team_1 else 't2_deltas'
            key_new   = 't1_new_vals' if is_team_1 else 't2_new_vals'
            key_sum   = 't1_gain_sum' if is_team_1 else 't2_gain_sum'
            
            deltas_c = res_curr[key_delta]
            new_c    = res_curr[key_new]
            deltas_f = res_fs[key_delta]
            new_f    = res_fs[key_new]
            
            num_opp = len(deltas_c)
            
            avg_delta_c = res_curr[key_sum] / num_opp if num_opp > 0 else 0
            avg_delta_f = res_fs[key_sum] / num_opp if num_opp > 0 else 0
            
            base_c_sum = sum(baseline_data['Current'][team_name][o] for o in deltas_c.keys())
            base_f_sum = sum(baseline_data['FullStrength'][team_name][o] for o in deltas_f.keys())
            
            avg_base_c = base_c_sum / num_opp if num_opp > 0 else 0
            avg_base_f = base_f_sum / num_opp if num_opp > 0 else 0
            avg_new_c  = avg_base_c + avg_delta_c
            avg_new_f  = avg_base_f + avg_delta_f

            print(f"{team_name:<18} | {'CURR':^20} | {'FULL STRENGTH':^20}")
            print(f"{'Opponent':<18} | {'Base':>6} {'New':>6} {'Diff':>6} | {'Base':>6} {'New':>6} {'Diff':>6}")
            print(f"{'-'*68}")
            print(f"{'OVERALL (Avg)':<18} | {avg_base_c:>6.3f} {avg_new_c:>6.3f} {avg_delta_c:>+6.3f} | {avg_base_f:>6.3f} {avg_new_f:>6.3f} {avg_delta_f:>+6.3f}")
            print(f"{'-'*68}")

            for opp in sorted(deltas_c.keys()):
                dc = deltas_c[opp]
                nc = new_c[opp]
                bc = baseline_data['Current'][team_name][opp]
                df = deltas_f[opp]
                nf = new_f[opp]
                bf = baseline_data['FullStrength'][team_name][opp]
                
                print(f"vs {opp:<15} | {bc:>6.3f} {nc:>6.3f} {dc:>+6.3f} | {bf:>6.3f} {nf:>6.3f} {df:>+6.3f}")
            print() 

        print_team_table(team1_name, is_team_1=True)
        print_team_table(team2_name, is_team_1=False)


def analyze_exact_trade(
    team1_name: str,
    t1_players_out: List[str],
    team2_name: str,
    t2_players_out: List[str],
    t1_players_to_drop_post_trade: List[str], 
    t1_free_agents_to_add: List[str],
    rosters_map: dict,
    player_weekly_stats_map: dict,
    all_team_weekly_stats: dict,
    all_h2h_probs: dict,
    id_to_name_map: dict,
    n_sim_weeks: int = 5000
):
    """
    Analyzes one specific trade (t1_players_out <-> t2_players_out), 
    plus post-trade drops and FA pickups for Team 1.
    """
    
    # --- Imports for Free Agent Logic ---
    try:
        if team2_name == "FreeAgents":
            from database import SessionLocal, PlayerSeasonValue, Player
            from teams_and_players_lib import get_available_free_agents
            from game_picker_lib import (
                predict_all_player_probabilities,
                generate_weighted_game_samples
            )
    except ImportError:
        if team2_name == "FreeAgents":
             print("FATAL ERROR: Missing required modules for Free Agent trade analysis.")
             return

    # --- Print Header for New Analysis Type ---
    drop_fa_count = len(t1_players_to_drop_post_trade)
    fa_add_count = len(t1_free_agents_to_add)
    
    print(f"\n--- Analyzing Trade + Drops/FA: {team1_name} sends {len(t1_players_out)} for {len(t2_players_out)} from {team2_name} ---")
    if drop_fa_count > 0 or fa_add_count > 0:
        drop_names = ", ".join([id_to_name_map.get(pid, pid) for pid in t1_players_to_drop_post_trade])
        add_names = ", ".join([id_to_name_map.get(pid, pid) for pid in t1_free_agents_to_add])
        print(f"   Post-Trade Steps for {team1_name}: Drop [{drop_names}] (Total: {drop_fa_count}), Add FAs: [{add_names}] (Total: {fa_add_count})")

    t1_full_roster_tuples = rosters_map[team1_name]
    t2_full_roster_tuples = []
    
    # --- FREE AGENT HANDLING (Ensures T2/T1 FA stats are present) ---
    if team2_name == "FreeAgents":
        fa_players_to_check = [pid for pid in t2_players_out if pid not in player_weekly_stats_map or not player_weekly_stats_map[pid]]
        if fa_players_to_check:
             print(f"WARNING: Stats missing for {len(fa_players_to_check)} incoming T2 FAs. Results may be 0/inaccurate.")
        t2_full_roster_tuples = [(pid, 'Active') for pid in t2_players_out]
        if "FreeAgents" not in all_team_weekly_stats:
             all_team_weekly_stats["FreeAgents"] = {"FullStrength": [], "Current": []}
    else:
        t2_full_roster_tuples = rosters_map[team2_name]
    
    # --- Pre-Trade Status Map ---
    player_status_map = {}
    for pid, status in t1_full_roster_tuples:
        player_status_map[pid] = status
    for pid, status in t2_full_roster_tuples:
        player_status_map[pid] = status
        
    # --- Players involved in the secondary transactions for T1 ---
    t1_out_set = set(t1_players_out) # Players T1 sends in the trade
    t1_drops_set = set(t1_players_to_drop_post_trade) # Players T1 drops after the trade
    t1_fa_adds_set = set(t1_free_agents_to_add) # Players T1 adds from FA

    # Set of all players being removed from T1's full roster (Trade out + Drops)
    all_exiting_t1 = t1_out_set | t1_drops_set
    # Set of all players being added to T1's full roster (Trade in + FA adds)
    all_entering_t1 = set(t2_players_out) | t1_fa_adds_set

    # Check for players in the 'in' and 'out' list simultaneously
    overlap = all_exiting_t1 & all_entering_t1
    if overlap:
         note_players = [id_to_name_map.get(p, p) for p in overlap]
         print(f"NOTE: The following players are received in trade and immediately dropped: {', '.join(note_players)}. Final roster calculation accounts for this.")


    team_names = sorted(list(rosters_map.keys()))
    other_team_names = [t for t in team_names if t not in [team1_name, team2_name] and t != "FreeAgents"]
    scenarios_to_check = ["FullStrength", "Current"]
    
    # ... (Baseline calculation remains the same) ...
    
    baseline_data = {s: {team1_name: {}, team2_name: {}} for s in scenarios_to_check}
    for scenario in scenarios_to_check:
        for other in other_team_names:
            if (team1_name, other, scenario) in all_h2h_probs:
                baseline_data[scenario][team1_name][other] = all_h2h_probs[(team1_name, other, scenario)]['overall']
            if team2_name != "FreeAgents":
                if (team2_name, other, scenario) in all_h2h_probs:
                    baseline_data[scenario][team2_name][other] = all_h2h_probs[(team2_name, other, scenario)]['overall']
        
        if team2_name != "FreeAgents":
            if (team1_name, team2_name, scenario) in all_h2h_probs:
                base_t1_vs_t2 = all_h2h_probs[(team1_name, team2_name, scenario)]['overall']
                baseline_data[scenario][team1_name][team2_name] = base_t1_vs_t2
                if (team2_name, team1_name, scenario) in all_h2h_probs:
                    baseline_data[scenario][team2_name][team1_name] = all_h2h_probs[(team2_name, team1_name, scenario)]['overall']
                else:
                    baseline_data[scenario][team2_name][team1_name] = 1.0 - base_t1_vs_t2

    trade_results_by_scenario = {}
    
    # --- SIMULATION ---
    for scenario in scenarios_to_check:
        # 1. Get current base roster
        is_fs = (scenario == "FullStrength")
        t1_full_base_ids = set([pid for pid, status in t1_full_roster_tuples])
        t2_active_base = []
        if team2_name != "FreeAgents":
            t2_active_base = filter_roster(t2_full_roster_tuples, is_full_strength=is_fs)
        
        # 2. Construct new full roster IDs for Team 1
        t1_new_roster_all = t1_full_base_ids - all_exiting_t1
        net_entering_t1 = all_entering_t1 - all_exiting_t1 
        t1_new_roster_all.update(net_entering_t1)
        
        # Now apply the active filter (INJ/DROP status check) to the FINAL roster IDs
        t1_new_roster = []
        for p in t1_new_roster_all:
            # We must determine the final status of the player on Team 1
            current_status = player_status_map.get(p, 'Active') 
            
            # Key assumption: Players in t1_drops_set are designated 'DROP' status after the trade,
            # regardless of their initial status.
            if p in t1_drops_set:
                 current_status = 'DROP'
            
            if is_fs:
                if current_status != 'DROP':
                    t1_new_roster.append(p)
            else:
                if current_status != 'INJ':
                    t1_new_roster.append(p)

        # --- NEW PRINTING/LOGGING BLOCK FOR TEAM 1 START ---
        scenario_name = "FULL STRENGTH" if is_fs else "CURRENT"
        print(f"\n--- {team1_name} Roster after all moves ({scenario_name}) ---")
        
        # We need the player names for a readable output
        t1_roster_names = sorted([id_to_name_map.get(pid, pid) for pid in t1_new_roster])
        
        print(f"  Final Active Roster Size: {len(t1_new_roster)}")
        # Print roster names split over lines for long lists
        print("  Roster Players:")
        for i in range(0, len(t1_roster_names), 5):
            print(f"    {' | '.join(t1_roster_names[i:i+5])}")
        # --- NEW PRINTING/LOGGING BLOCK FOR TEAM 1 END ---
                
        # 3. Construct new active rosters for Team 2 (Trade logic only)
        t2_new_roster = []
        if team2_name != "FreeAgents":
            t2_new_roster = [p for p in t2_active_base if p not in t2_players_out]
            for p in t1_players_out:
                status = player_status_map.get(p)
                if is_fs:
                    if status != 'DROP': t2_new_roster.append(p)
                else:
                    if status != 'INJ': t2_new_roster.append(p)
            
            # --- NEW PRINTING/LOGGING BLOCK FOR TEAM 2 START ---
            t2_roster_names = sorted([id_to_name_map.get(pid, pid) for pid in t2_new_roster])
            
            print(f"\n--- {team2_name} Roster after trade ({scenario_name}) ---")
            print(f"  Final Active Roster Size: {len(t2_new_roster)}")
            print("  Roster Players:")
            for i in range(0, len(t2_roster_names), 5):
                print(f"    {' | '.join(t2_roster_names[i:i+5])}")
            # --- NEW PRINTING/LOGGING BLOCK FOR TEAM 2 END ---
        
        print("-" * 50) # Separator for scenarios

        # --- VALIDATION INJECTION ---
        validate_and_crash_if_invalid_roster(team1_name, t1_new_roster, scenario, id_to_name_map)
        if team2_name != "FreeAgents":
            validate_and_crash_if_invalid_roster(team2_name, t2_new_roster, scenario, id_to_name_map)
        # ----------------------------

        # Fast Sim
        t1_new_weeks = build_team_weeks_from_players(t1_new_roster, player_weekly_stats_map, n_sim_weeks)
        t2_new_weeks = []
        if team2_name != "FreeAgents":
            t2_new_weeks = build_team_weeks_from_players(t2_new_roster, player_weekly_stats_map, n_sim_weeks)
        
        # ... (Remaining calculation logic is unchanged) ...
        # Stats containers
        t1_deltas = {}
        t2_deltas = {}
        t1_new_vals = {}
        t2_new_vals = {}
        
        t1_gain_sum = 0.0
        t2_gain_sum = 0.0
        
        # A. Compare against Rest of League
        for other in other_team_names:
            other_weeks = all_team_weekly_stats[other][scenario]
            
            # Calc Team 1
            new_win_pct_1 = compare_n_weeks(t1_new_weeks, other_weeks)['overall']
            base_1 = baseline_data[scenario][team1_name].get(other, 0.5)
            d1 = new_win_pct_1 - base_1
            t1_deltas[other] = d1
            t1_new_vals[other] = new_win_pct_1
            t1_gain_sum += d1
            
            # Calc Team 2 (Only if real team)
            if team2_name != "FreeAgents":
                new_win_pct_2 = compare_n_weeks(t2_new_weeks, other_weeks)['overall']
                base_2 = baseline_data[scenario][team2_name].get(other, 0.5)
                d2 = new_win_pct_2 - base_2
                t2_deltas[other] = d2
                t2_new_vals[other] = new_win_pct_2
                t2_gain_sum += d2
            else:
                t2_deltas[other] = 0.0
                t2_new_vals[other] = 0.0
        
        # B. Compare Team 1 vs Team 2 (Post Trade)
        if team2_name != "FreeAgents":
            new_win_pct_1_vs_2 = compare_n_weeks(t1_new_weeks, t2_new_weeks)['overall']
            base_1_vs_2 = baseline_data[scenario][team1_name].get(team2_name, 0.5)
            d1_vs_2 = new_win_pct_1_vs_2 - base_1_vs_2
            
            t1_deltas[team2_name] = d1_vs_2
            t1_new_vals[team2_name] = new_win_pct_1_vs_2
            t1_gain_sum += d1_vs_2
            
            new_win_pct_2_vs_1 = 1.0 - new_win_pct_1_vs_2
            base_2_vs_1 = baseline_data[scenario][team2_name].get(team1_name, 0.5)
            d2_vs_1 = new_win_pct_2_vs_1 - base_2_vs_1
            
            t2_deltas[team1_name] = d2_vs_1
            t2_new_vals[team1_name] = new_win_pct_2_vs_1
            t2_gain_sum += d2_vs_1

        trade_results_by_scenario[scenario] = {
            "t1_gain_sum": t1_gain_sum,
            "t2_gain_sum": t2_gain_sum,
            "t1_deltas": t1_deltas,
            "t2_deltas": t2_deltas,
            "t1_new_vals": t1_new_vals,
            "t2_new_vals": t2_new_vals
        }

    # 3. Report Results (Unchanged)
    
    def get_player_names(player_ids):
        return [id_to_name_map.get(pid, pid) for pid in player_ids]
        
    t1_gives_names = ", ".join(get_player_names(t1_players_out))
    t2_gives_names = ", ".join(get_player_names(t2_players_out))
    t1_drops_names = ", ".join(get_player_names(t1_players_to_drop_post_trade))
    t1_adds_names = ", ".join(get_player_names(t1_free_agents_to_add))
    
    combined_gain = 0
    for scen in scenarios_to_check:
        combined_gain += trade_results_by_scenario[scen]['t1_gain_sum']
        if team2_name != "FreeAgents":
            combined_gain += trade_results_by_scenario[scen]['t2_gain_sum']
            
    print(f"\n{'='*85}")
    print(f"EXACT TRADE ANALYSIS:")
    print(f"  STEP 1: {team1_name} trades [{t2_gives_names}] <--> {team2_name} trades [{t1_gives_names}]")
    if t1_drops_names or t1_adds_names:
        print(f"  STEP 2: {team1_name} Drops [{t1_drops_names}] and Adds FAs: [{t1_adds_names}]")
    print(f"Combined League Gain Metric: {combined_gain:.4f}")
    print(f"{'-'*85}")

    def print_team_table(team_name, is_team_1, trade_results):
        if team_name == "FreeAgents":
            return

        res_curr = trade_results['Current']
        res_fs   = trade_results['FullStrength']
        
        key_delta = 't1_deltas' if is_team_1 else 't2_deltas'
        key_new   = 't1_new_vals' if is_team_1 else 't2_new_vals'
        key_sum   = 't1_gain_sum' if is_team_1 else 't2_gain_sum'
        
        deltas_c = res_curr[key_delta]
        new_c    = res_curr[key_new]
        deltas_f = res_fs[key_delta]
        new_f    = res_fs[key_new]
        
        num_opp = len(deltas_c)
        
        avg_delta_c = res_curr[key_sum] / num_opp if num_opp > 0 else 0
        avg_delta_f = res_fs[key_sum] / num_opp if num_opp > 0 else 0
        
        # Calculate baseline averages
        base_c_sum = sum(baseline_data['Current'][team_name].get(o, 0.5) for o in deltas_c.keys())
        base_f_sum = sum(baseline_data['FullStrength'][team_name].get(o, 0.5) for o in deltas_f.keys())
        
        avg_base_c = base_c_sum / num_opp if num_opp > 0 else 0
        avg_base_f = base_f_sum / num_opp if num_opp > 0 else 0
        avg_new_c  = avg_base_c + avg_delta_c
        avg_new_f  = avg_base_f + avg_delta_f

        print(f"{team_name:<18} | {'CURR':^20} | {'FULL STRENGTH':^20}")
        print(f"{'Opponent':<18} | {'Base':>6} {'New':>6} {'Diff':>6} | {'Base':>6} {'New':>6} {'Diff':>6}")
        print(f"{'-'*68}")
        print(f"{'OVERALL (Avg)':<18} | {avg_base_c:>6.3f} {avg_new_c:>6.3f} {avg_delta_c:>+6.3f} | {avg_base_f:>6.3f} {avg_new_f:>6.3f} {avg_delta_f:>+6.3f}")
        print(f"{'-'*68}")

        for opp in sorted(deltas_c.keys()):
            bc = baseline_data['Current'][team_name].get(opp, 0.5)
            bf = baseline_data['FullStrength'][team_name].get(opp, 0.5)
            
            dc = deltas_c[opp]
            nc = new_c[opp]
            df = deltas_f[opp]
            nf = new_f[opp]
            
            print(f"vs {opp:<15} | {bc:>6.3f} {nc:>6.3f} {dc:>+6.3f} | {bf:>6.3f} {nf:>6.3f} {df:>+6.3f}")
        print() 

    print_team_table(team1_name, is_team_1=True, trade_results=trade_results_by_scenario)
    print_team_table(team2_name, is_team_1=False, trade_results=trade_results_by_scenario)
    
    return trade_results_by_scenario