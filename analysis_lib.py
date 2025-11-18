import time
from typing import Dict, List, Tuple, Optional, Set
from itertools import combinations

# --- Constants ---
EXPECTED_ROSTER_SIZE = 13
CATEGORIES = ['pts', 'reb', 'ast', 'stl', 'blk', 'tpm', 'to', 'fg%', 'ft%']
ALL_STAT_KEYS = ['pts', 'reb', 'ast', 'stl', 'blk', 'tpm', 'to', 'fga', 'fgm', 'fta', 'ftm']

# --- Helper Functions ---

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
            
            # Optional: Check roster size here if desired, removed for brevity in lib file
            
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
    rosters_map: Dict,
    player_weekly_stats_map: Dict,
    all_team_weekly_stats: Dict,
    all_h2h_probs: Dict,
    id_to_name_map: Dict,
    team2_loss_tolerance: float = 0.0,
    allow_trading_injured: bool = True,
    n_sim_weeks: int = 5000
):
    """
    Finds trades where Team 1 improves and Team 2's loss is within tolerance.
    """
    print(f"\n--- Step 4: Finding best {n}-for-{n} trades for {team1_name} & {team2_name} ---")
    print(f"Config: T2 Tolerance = -{team2_loss_tolerance} | Allow Injured Trades = {allow_trading_injured}")
    
    t1_full_roster_tuples = rosters_map[team1_name]
    t2_full_roster_tuples = rosters_map[team2_name]
    
    successful_trades = []
    team_names = sorted(list(rosters_map.keys()))
    other_team_names = [t for t in team_names if t not in [team1_name, team2_name]]
    
    scenarios_to_check = ["FullStrength", "Current"]

    # 1. Calculate the baseline stats
    baseline_data = {s: {team1_name: {}, team2_name: {}} for s in scenarios_to_check}
    for scenario in scenarios_to_check:
        for other in other_team_names:
            baseline_data[scenario][team1_name][other] = all_h2h_probs[(team1_name, other, scenario)]['overall']
            baseline_data[scenario][team2_name][other] = all_h2h_probs[(team2_name, other, scenario)]['overall']

    # 2. Identify TRADABLE players
    def get_tradable_players(roster_tuples):
        tradable = []
        for pid, status in roster_tuples:
            if status == 'DROP': continue
            if not allow_trading_injured and status == 'INJ': continue
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
            is_trade_valid = True
            trade_results_by_scenario = {}
            
            if checked_count % 5000 == 0:
                 print(f" ... checked {checked_count}/{total_trades_to_check}")

            for scenario in scenarios_to_check:
                t1_roster = filter_roster(t1_full_roster_tuples, is_full_strength=(scenario=="FullStrength"))
                t2_roster = filter_roster(t2_full_roster_tuples, is_full_strength=(scenario=="FullStrength"))
                
                t1_new_roster = [p for p in t1_roster if p not in t1_players_out] + list(t2_players_out)
                t2_new_roster = [p for p in t2_roster if p not in t2_players_out] + list(t1_players_out)
                
                # Fast Sim
                t1_new_weeks = build_team_weeks_from_players(t1_new_roster, player_weekly_stats_map, n_sim_weeks)
                t2_new_weeks = build_team_weeks_from_players(t2_new_roster, player_weekly_stats_map, n_sim_weeks)
                
                # Stats containers
                t1_deltas = {}
                t2_deltas = {}
                t1_new_vals = {}
                t2_new_vals = {} 
                
                t1_gain_sum = 0.0
                t2_gain_sum = 0.0
                
                for other in other_team_names:
                    other_weeks = all_team_weekly_stats[other][scenario]
                    
                    new_win_pct_1 = compare_n_weeks(t1_new_weeks, other_weeks)['overall']
                    new_win_pct_2 = compare_n_weeks(t2_new_weeks, other_weeks)['overall']
                    
                    base_1 = baseline_data[scenario][team1_name][other]
                    base_2 = baseline_data[scenario][team2_name][other]
                    
                    d1 = new_win_pct_1 - base_1
                    d2 = new_win_pct_2 - base_2
                    
                    t1_deltas[other] = d1
                    t2_deltas[other] = d2
                    t1_new_vals[other] = new_win_pct_1
                    t2_new_vals[other] = new_win_pct_2
                    
                    t1_gain_sum += d1
                    t2_gain_sum += d2

                # Check Criteria
                if t1_gain_sum <= 0 or t2_gain_sum < -team2_loss_tolerance:
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