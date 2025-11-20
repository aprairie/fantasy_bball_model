import random
import time
import argparse
import sys
from typing import Dict, List, Set
from sqlalchemy.orm import Session
from sqlalchemy import exc

# --- Library Imports ---
import analysis_lib as analysis

try:
    from database import SessionLocal, init_db, Player, GameStats
    from teams_and_players_lib import get_league_rosters
    from game_picker_lib import (
        predict_all_player_probabilities,
        generate_weighted_game_samples,
        save_predictions_to_db,
        calculate_all_player_values
    )
    # NEW: Import the unified scraper library
    from scraper_lib import BasketballScraper
    # NEW: Import the print output library
    from print_output_lib import export_player_stats_to_csv
    
except ImportError as e:
    print(f"FATAL ERROR: Could not import libraries: {e}")
    print("Ensure database.py, teams_and_players_lib.py, game_picker_lib.py, analysis_lib.py, scraper_lib.py and print_output_lib.py are present.")
    sys.exit(1)

# --- Simulation Configuration ---
N_GAMES_TO_GENERATE = 10000
N_SIM_WEEKS = 5000
SIM_YEAR_WEIGHTS = [(2026, 1.2), (2025, 0.2), (2024, 0.1)]
AVAILABILITY_SIM_YEAR_WEIGHTS = [(2026, 1), (2025, 1), (2024, 1)]
PRIOR_PLAY_PERCENTAGE = 0.85
PRIOR_STRENGTH_IN_GAMES = 82.0
ALL_STAT_KEYS = ['pts', 'reb', 'ast', 'stl', 'blk', 'tpm', 'to', 'fga', 'fgm', 'fta', 'ftm']

# --- Data Generation Functions ---

def get_all_player_game_pools(
    session: Session,
    player_id_set: Set[str]
) -> Dict[str, List[GameStats]]:
    """Generates the game pool for every player."""
    print(f"\n--- Step 1: Generating {N_GAMES_TO_GENERATE}-game pools for {len(player_id_set)} players ---")
    start_time = time.time()
    
    print("Calculating player availability...")
    availability_map = predict_all_player_probabilities(
        session,
        AVAILABILITY_SIM_YEAR_WEIGHTS,
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
    """Pre-simulates weeks for every player individually."""
    print(f"\n--- Step 2: Pre-simulating {num_weeks} weeks for {len(player_id_set)} players ---")
    start_time = time.time()
    
    player_weekly_stats_map = {player_id: [] for player_id in player_id_set}
    
    for player_id in player_id_set:
        player_pool = game_pools.get(player_id)
        player_weeks = []
        
        if not player_pool:
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

# --- Argument Parsing ---

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Fantasy Basketball Simulation & Trade Analyzer",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available Commands', required=True)

    # --- Command: scrape ---
    parser_scrape = subparsers.add_parser(
        'scrape', 
        help='Scrape game logs or player data from Basketball-Reference'
    )
    parser_scrape.add_argument(
        '--birthdays', 
        action='store_true', 
        help='Scrape missing player birth dates.'
    )
    parser_scrape.add_argument(
        '--years', 
        nargs='+', 
        type=int, 
        metavar='YYYY',
        help='A list of years (e.g., 2026 2025) to scrape game logs for.'
    )

    # --- Command: availability ---
    parser_avail = subparsers.add_parser(
        'availability', 
        help='Calculate and save player availability (Season=1)'
    )
    
    # --- Command: values ---
    parser_values = subparsers.add_parser(
        'values', 
        help='Calculate and save player values (Z-scores) for Real (2026) and Simulated (Season=1) data'
    )
    
    # --- Command: export (NEW) ---
    parser_export = subparsers.add_parser(
        'export', 
        help='Export calculated player stats to CSV (prints to stdout)'
    )

    # --- Command: h2h ---
    parser_h2h = subparsers.add_parser(
        'h2h', 
        help='Run full league Head-to-Head analysis'
    )

    # --- Command: trade ---
    parser_trade = subparsers.add_parser(
        'trade', 
        help='Find optimal trades between two teams'
    )
    parser_trade.add_argument(
        '--team1', 
        required=True, 
        type=str, 
        help='Name of the first team (The Improver)'
    )
    parser_trade.add_argument(
        '--team2', 
        required=True, 
        type=str, 
        help='Name of the second team (The Partner)'
    )
    parser_trade.add_argument(
        '-n', '--num', 
        type=int, 
        default=2, 
        help='Number of players to trade per side (default: 2)'
    )
    parser_trade.add_argument(
        '-t', '--tolerance', 
        type=float, 
        default=0.05, 
        help='Loss tolerance for Team 2 (default: 0.05)'
    )
    parser_trade.add_argument(
        '--injured', 
        action='store_true', 
        help='Allow trading injured players'
    )

    # Handle case where no command is given
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    return parser.parse_args(), parser

# --- Main Execution ---

if __name__ == "__main__":
    args, parser = parse_arguments()

    # Don't print start banners if exporting, to keep CSV clean
    if args.command != 'export':
        print(f"--- Starting Fantasy League Simulator (Mode: {args.command.upper()}) ---")
    
    session = None
    try:
        session = SessionLocal()
        init_db()
        # Only print connection success if not exporting
        if args.command != 'export':
            print("Database connection successful.")
    except Exception as e:
        # Errors always go to stderr so they don't corrupt the CSV
        print(f"Database connection failed: {e}", file=sys.stderr)
        sys.exit(1)
    
    try:
        # --- COMMAND: SCRAPE ---
        if args.command == 'scrape':
            if not args.birthdays and not args.years:
                print("Error: Please specify what to scrape.", file=sys.stderr)
                for action in parser._actions:
                    if isinstance(action, argparse._SubParsersAction):
                        action.choices['scrape'].print_help()
                        break
                sys.exit(1)
            
            scraper = BasketballScraper()
            
            if args.birthdays:
                scraper.scrape_all_player_birthdays(session)
            
            if args.years:
                sorted_years = sorted(list(set(args.years)), reverse=True)
                for year in sorted_years:
                    scraper.scrape_game_logs_for_season(session, year)
            
            print("--- Scraping command finished. ---")

        # --- COMMAND: AVAILABILITY ---
        elif args.command == 'availability':
            print(f"Calculating probabilities using weights: {AVAILABILITY_SIM_YEAR_WEIGHTS}")
            
            all_probs = predict_all_player_probabilities(
                session,
                AVAILABILITY_SIM_YEAR_WEIGHTS,
                PRIOR_PLAY_PERCENTAGE,
                PRIOR_STRENGTH_IN_GAMES
            )
            
            if all_probs:
                save_predictions_to_db(session, all_probs)
                print("\n--- Top 5 Highest Availability ---")
                sorted_probs = sorted(all_probs.items(), key=lambda x: x[1], reverse=True)
                for pid, prob in sorted_probs[:5]:
                    print(f"{pid:<15} : {prob:.1%}")
                print("\n--- Top 5 Lowest Availability ---")
                for pid, prob in sorted_probs[-5:]:
                    print(f"{pid:<15} : {prob:.1%}")
            else:
                print("No probability data generated.")
        
        # --- COMMAND: VALUES ---
        elif args.command == 'values':
            calculate_all_player_values(session)

        # --- COMMAND: EXPORT (NEW) ---
        elif args.command == 'export':
            export_player_stats_to_csv(session)

        # --- COMMAND: H2H or TRADE ---
        elif args.command in ('h2h', 'trade'):
            rosters_map = get_league_rosters()
            if not rosters_map:
                raise Exception("Could not get league rosters.")
                
            if args.command == 'trade':
                valid_teams = set(rosters_map.keys())
                if args.team1 not in valid_teams:
                    print(f"Error: '{args.team1}' is not a valid team.", file=sys.stderr)
                    sys.exit(1)
                if args.team2 not in valid_teams:
                    print(f"Error: '{args.team2}' is not a valid team.", file=sys.stderr)
                    sys.exit(1)
                if args.team1 == args.team2:
                    print("Error: Team 1 and Team 2 must be different.", file=sys.stderr)
                    sys.exit(1)

            all_player_ids_set = set(pid for r in rosters_map.values() for pid, s in r)
            
            player_map_query = session.query(Player.player_id, Player.name).all()
            id_to_name_map = {p.player_id: p.name for p in player_map_query}
            
            game_pools = get_all_player_game_pools(session, all_player_ids_set)
            player_weekly_stats_map = pre_simulate_player_weeks(
                game_pools,
                all_player_ids_set,
                N_SIM_WEEKS
            )

            if args.command == 'h2h':
                analysis.run_league_simulation(
                    rosters_map, player_weekly_stats_map, id_to_name_map, N_SIM_WEEKS
                )
                
            elif args.command == 'trade':
                print("\n(Calculating baseline league stats first...)")
                all_team_weekly_stats, all_h2h_probs = analysis.run_league_simulation(
                    rosters_map, player_weekly_stats_map, id_to_name_map, N_SIM_WEEKS
                )
                
                analysis.find_trades(
                    n=args.num,
                    team1_name=args.team1,
                    team2_name=args.team2,
                    rosters_map=rosters_map,
                    player_weekly_stats_map=player_weekly_stats_map,
                    all_team_weekly_stats=all_team_weekly_stats,
                    all_h2h_probs=all_h2h_probs,
                    id_to_name_map=id_to_name_map,
                    team2_loss_tolerance=args.tolerance,
                    allow_trading_injured=args.injured,
                    n_sim_weeks=N_SIM_WEEKS
                )

    except exc.SQLAlchemyError as e:
        print(f"A database error occurred: {e}", file=sys.stderr)
        if session:
            session.rollback()
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        if session:
            session.close()
            # Only print closing message if not exporting
            if args.command != 'export':
                print("\nDatabase session closed.")