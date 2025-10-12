import random
import itertools
from collections import defaultdict
from sqlalchemy.orm import joinedload
from database import SessionLocal, Player, GameStats, EloStats, SimulationInfo
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Simulation Constants ---
NUM_SIMULATIONS = 50000
NUM_TEAMS = 10
TEAM_SIZE = 13
INITIAL_ELO = 1500.0
K_FACTOR = 5  # Lowered the update factor a lot, since we can run many simulations.

# The 9 fantasy categories and their corresponding ELO attribute names
CATEGORIES = {
    'pts': 'pts_elo',
    'reb': 'reb_elo',
    'ast': 'ast_elo',
    'stl': 'stl_elo',
    'blk': 'blk_elo',
    'tpm': 'tpm_elo',
    'to': 'to_elo',
    'fg_pct': 'fg_pct_elo',
    'ft_pct': 'ft_pct_elo'
}

def calculate_expected_outcome(team_avg_elo, opponent_avg_elo):
    """Calculates the probability of a team winning based on ELO ratings."""
    return 1 / (1 + 10 ** ((opponent_avg_elo - team_avg_elo) / 400))

def update_elos(winner_team, loser_team, elo_data, category_key):
    """Updates ELO ratings for all players on winning and losing teams for a specific category."""
    # 1. Ensure teams are not empty to avoid division by zero
    if not winner_team or not loser_team:
        return

    # 2. Calculate average team ELOs for the specific category
    avg_elo_winner = sum(elo_data[p.id][category_key] for p in winner_team) / len(winner_team)
    avg_elo_loser = sum(elo_data[p.id][category_key] for p in loser_team) / len(loser_team)

    # 3. Calculate expected outcome
    expected_win_prob = calculate_expected_outcome(avg_elo_winner, avg_elo_loser)

    # 4. Calculate ELO change
    elo_change = K_FACTOR * (1 - expected_win_prob)

    # 5. Apply the change to each player
    for player in winner_team:
        elo_data[player.id][category_key] += elo_change
    for player in loser_team:
        elo_data[player.id][category_key] -= elo_change

def get_random_game_stats(player_id, player_games):
    """
    Selects a single random game from a player's history and returns its stats.
    Returns a dictionary of stats, with 0.0 for any missing data points.
    """
    if not player_games.get(player_id):
        return None

    game = random.choice(player_games[player_id])
    if game.points is None:
        return None
    
    # Return a dictionary with stats, handling potential None values
    return {
        'pts': game.points or 0.0,
        'reb': game.total_rebounds or 0.0,
        'ast': game.assists or 0.0,
        'stl': game.steals or 0.0,
        'blk': game.blocks or 0.0,
        'tpm': game.three_pointers or 0.0,
        'to': game.turnovers or 0.0,
        'fgm': game.field_goals or 0.0,
        'fga': game.field_goal_attempts or 0.0,
        'ftm': game.free_throws or 0.0,
        'fta': game.free_throw_attempts or 0.0,
    }

def aggregate_team_stats(team, player_games):
    """
    Aggregates the stats for an entire team for a weekly matchup.
    Each player contributes stats from a random 3 or 4 of their games.
    """
    team_stats = defaultdict(float)
    for player in team:
        # Simulate a player playing 3 or 4 games in a fantasy week
        num_games = random.randint(3, 4)
        for _ in range(num_games):
            game_stats = get_random_game_stats(player.id, player_games)
            if game_stats is None:
                continue
            
            # Sum up the stats from the selected game
            for stat, value in game_stats.items():
                team_stats[stat] += value
    
    # Calculate percentage stats after aggregating counting stats
    team_stats['fg_pct'] = team_stats['fgm'] / team_stats['fga'] if team_stats['fga'] else 0
    team_stats['ft_pct'] = team_stats['ftm'] / team_stats['fta'] if team_stats['fta'] else 0
    return team_stats

### CHANGE: Added a new function for the "guaranteed games" simulation.
def aggregate_team_stats_guaranteed(team, player_games):
    """
    Aggregates team stats, ensuring that a player always contributes stats for their
    simulated games by re-drawing if a selected game is missing.
    """
    team_stats = defaultdict(float)
    for player in team:
        num_games = random.randint(3, 4)
        for _ in range(num_games):
            game_stats = None
            # This loop ensures we always find a game for the player,
            # assuming they have at least one game in their history.
            while not game_stats:
                game_stats = get_random_game_stats(player.id, player_games)

            
            for stat, value in game_stats.items():
                team_stats[stat] += value
    
    team_stats['fg_pct'] = team_stats['fgm'] / team_stats['fga'] if team_stats['fga'] else 0
    team_stats['ft_pct'] = team_stats['ftm'] / team_stats['fta'] if team_stats['fta'] else 0
    return team_stats

def play_matchup(team_a, team_b, elo_data, player_games, guaranteed_games=False):
    """
    Simulates a matchup between two teams, determines category winners, and updates ELOs.
    Can use either standard or guaranteed stat aggregation.
    """
    if guaranteed_games:
        team_a_stats = aggregate_team_stats_guaranteed(team_a, player_games)
        team_b_stats = aggregate_team_stats_guaranteed(team_b, player_games)
    else:
        team_a_stats = aggregate_team_stats(team_a, player_games)
        team_b_stats = aggregate_team_stats(team_b, player_games)

    team_a_score = 0
    team_b_score = 0

    # Determine winners for each category
    for cat_name, elo_key in CATEGORIES.items():
        val_a = team_a_stats[cat_name]
        val_b = team_b_stats[cat_name]
        
        is_turnover_cat = (cat_name == 'to')
        
        winner, loser = (None, None)
        
        if (is_turnover_cat and val_a < val_b) or (not is_turnover_cat and val_a > val_b):
            winner, loser = team_a, team_b
            team_a_score += 1
        elif (is_turnover_cat and val_b < val_a) or (not is_turnover_cat and val_b > val_a):
            winner, loser = team_b, team_a
            team_b_score += 1
        
        if winner and loser:
            update_elos(winner, loser, elo_data, elo_key)

    # Update overall ELO based on the final matchup score
    if team_a_score > team_b_score:
        update_elos(team_a, team_b, elo_data, 'overall_elo')
    elif team_b_score > team_a_score:
        update_elos(team_b, team_a, elo_data, 'overall_elo')


def run_simulations():
    """Main function to orchestrate the fantasy basketball ELO simulation with a player culling phase."""
    db = SessionLocal()
    try:
        # --- Initial Data Loading ---
        logger.info("Loading player and game data from the database...")
        all_players = db.query(Player).options(joinedload(Player.game_stats)).all()
        players_with_games = [p for p in all_players if p.game_stats]
        
        TARGET_PLAYER_COUNT = 200
        if len(players_with_games) <= TARGET_PLAYER_COUNT:
            logger.info(f"Initial player count is at or below the target of {TARGET_PLAYER_COUNT}. Skipping culling phase.")
        else:
            # --- Culling Phase ---
            logger.info("--- Starting Player Culling Phase ---")
            CULLING_SIMULATIONS_PER_CYCLE = 2500
            PLAYERS_TO_DROP_PER_CYCLE = 20

            while len(players_with_games) > TARGET_PLAYER_COUNT:
                current_player_count = len(players_with_games)
                logger.info(f"\nStarting culling cycle with {current_player_count} players...")
                
                player_games = {p.id: p.game_stats for p in players_with_games}

                # Reset ELO ratings for all players at the start of the cycle
                elo_data = {}
                for player in players_with_games:
                    elo_data[player.id] = {key: INITIAL_ELO for key in CATEGORIES.values()}
                    elo_data[player.id]['overall_elo'] = INITIAL_ELO

                # Run a smaller number of simulations for this culling cycle
                for i in range(CULLING_SIMULATIONS_PER_CYCLE):
                    available_players = sorted(
                        players_with_games, 
                        key=lambda p: elo_data.get(p.id, {}).get('overall_elo', INITIAL_ELO),
                        reverse=True
                    )
                    
                    teams = [[] for _ in range(NUM_TEAMS)]
                    for round_num in range(TEAM_SIZE):
                        team_order = range(NUM_TEAMS) if round_num % 2 == 0 else reversed(range(NUM_TEAMS))
                        for team_idx in team_order:
                            if available_players:
                                # Pick a random player
                                random_index = random.randrange(len(available_players))

                                # Pop the item at that index and store it in the team.
                                teams[team_idx].append(available_players.pop(random_index))
                                # teams[team_idx].append(available_players.pop(0))

                    matchups = itertools.combinations(teams, 2)
                    for team_a, team_b in matchups:
                        play_matchup(team_a, team_b, elo_data, player_games)

                # Identify and drop the lowest-ranked players
                logger.info("Culling cycle complete. Identifying lowest-ranked players...")
                sorted_by_elo = sorted(players_with_games, key=lambda p: elo_data[p.id]['overall_elo'])
                
                num_to_drop = min(PLAYERS_TO_DROP_PER_CYCLE, current_player_count - TARGET_PLAYER_COUNT)
                if num_to_drop <= 0: break

                ### CHANGE: Identify and list the players being culled.
                players_to_drop = sorted_by_elo[:num_to_drop]
                logger.info(f"--- Dropping {len(players_to_drop)} Players ---")
                for p in players_to_drop:
                    player_elo = elo_data[p.id]['overall_elo']
                    logger.info(f"  - {p.name} (Elo: {player_elo:.2f})")

                players_with_games = sorted_by_elo[num_to_drop:]
                logger.info(f"{len(players_with_games)} players remaining.")

        # --- Final Simulation Runs ---
        logger.info(f"\n--- Culling complete. Final player pool size: {len(players_with_games)} ---")
        player_games = {p.id: p.game_stats for p in players_with_games}
        
        ### CHANGE: Run the main simulation twice.
        
        # --- Run 1: Standard Simulation ---
        logger.info("\n--- Starting Final Simulation (Standard Model) ---")
        elo_data_standard = {}
        for player in players_with_games:
            elo_data_standard[player.id] = {key: INITIAL_ELO for key in CATEGORIES.values()}
            elo_data_standard[player.id]['overall_elo'] = INITIAL_ELO

        for i in range(NUM_SIMULATIONS):
            if (i + 1) % 1000 == 0:
                logger.info(f"Running Standard Simulation {i + 1}/{NUM_SIMULATIONS}...")
            available_players = sorted(
                players_with_games, 
                key=lambda p: elo_data_standard.get(p.id, {}).get('overall_elo', INITIAL_ELO),
                reverse=True
            )
            teams = [[] for _ in range(NUM_TEAMS)]
            for round_num in range(TEAM_SIZE):
                team_order = range(NUM_TEAMS) if round_num % 2 == 0 else reversed(range(NUM_TEAMS))
                for team_idx in team_order:
                    if available_players:
                        teams[team_idx].append(available_players.pop(0))
            matchups = itertools.combinations(teams, 2)
            for team_a, team_b in matchups:
                play_matchup(team_a, team_b, elo_data_standard, player_games, guaranteed_games=False)
        
        # --- Run 2: Guaranteed Games Simulation ---
        logger.info("\n--- Starting Final Simulation (Guaranteed Games Model) ---")
        elo_data_guaranteed = {}
        for player in players_with_games:
            elo_data_guaranteed[player.id] = {key: INITIAL_ELO for key in CATEGORIES.values()}
            elo_data_guaranteed[player.id]['overall_elo'] = INITIAL_ELO
        
        for i in range(NUM_SIMULATIONS):
            if (i + 1) % 1000 == 0:
                logger.info(f"Running Guaranteed Simulation {i + 1}/{NUM_SIMULATIONS}...")
            available_players = sorted(
                players_with_games, 
                key=lambda p: elo_data_guaranteed.get(p.id, {}).get('overall_elo', INITIAL_ELO),
                reverse=True
            )
            teams = [[] for _ in range(NUM_TEAMS)]
            for round_num in range(TEAM_SIZE):
                team_order = range(NUM_TEAMS) if round_num % 2 == 0 else reversed(range(NUM_TEAMS))
                for team_idx in team_order:
                    if available_players:
                        teams[team_idx].append(available_players.pop(0))
            matchups = itertools.combinations(teams, 2)
            for team_a, team_b in matchups:
                play_matchup(team_a, team_b, elo_data_guaranteed, player_games, guaranteed_games=True)

        # --- Update Database After All Simulations ---
        logger.info("\nAll simulations complete. Updating the database with both sets of ratings...")
        
        final_player_ids = elo_data_standard.keys()
        for player_id in final_player_ids:
            elo_record = db.query(EloStats).filter(EloStats.player_id == player_id).first()
            if not elo_record:
                elo_record = EloStats(player_id=player_id)
                db.add(elo_record)
            
            # Assign updated values from the standard simulation
            standard_ratings = elo_data_standard[player_id]
            for key, value in standard_ratings.items():
                setattr(elo_record, key, value)
                
            # Assign updated values from the guaranteed games simulation
            guaranteed_ratings = elo_data_guaranteed[player_id]
            for key, value in guaranteed_ratings.items():
                ### CHANGE: Save to new DB columns with a '_guaranteed' suffix
                setattr(elo_record, key + '_guaranteed', value)

        # Update simulation count
        sim_info = db.query(SimulationInfo).first()
        if not sim_info:
            sim_info = SimulationInfo(simulation_count=0)
            db.add(sim_info)
        
        sim_info.simulation_count += NUM_SIMULATIONS
        
        db.commit()
        logger.info("Database has been successfully updated with new ELO ratings for the top players.")

    finally:
        db.close()

if __name__ == "__main__":
    run_simulations()