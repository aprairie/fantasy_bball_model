import random
import itertools
from collections import defaultdict
from sqlalchemy.orm import joinedload
from database import SessionLocal, Player, GameStats, EloStats, SimulationInfo
from pprint import pprint
import random


# --- Simulation Constants ---
NUM_SIMULATIONS = 10000
NUM_TEAMS = 10
TEAM_SIZE = 13
INITIAL_ELO = 1500.0
K_FACTOR = 5  # Lowered the update factor a lot, since we can run many simulations.

# The 9 fantasy categories
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
    """Updates ELO ratings for all players on winning and losing teams."""
    # 1. Calculate average team ELOs for the specific category
    avg_elo_winner = sum(elo_data[p.id][category_key] for p in winner_team) / len(winner_team)
    avg_elo_loser = sum(elo_data[p.id][category_key] for p in loser_team) / len(loser_team)

    # 2. Calculate expected outcome
    expected_win_prob = calculate_expected_outcome(avg_elo_winner, avg_elo_loser)

    # 3. Calculate ELO change
    elo_change = K_FACTOR * (1 - expected_win_prob)

    # 4. Apply the change to each player
    for player in winner_team:
        elo_data[player.id][category_key] += elo_change
    for player in loser_team:
        elo_data[player.id][category_key] -= elo_change

def get_game(player_id, player_games):
    result_dict = {}
    if not player_games[player_id]:
        return None
    game = random.choice(player_games[player_id])
    result_dict['points'] = 0.0 if game.points is None else game.points
    result_dict['reb'] = 0.0 if game.total_rebounds is None else game.total_rebounds
    result_dict['ast'] = 0.0 if game.assists is None else game.assists
    result_dict['stl'] = 0.0 if game.steals is None else game.steals
    result_dict['blk'] = 0.0 if game.blocks is None else game.blocks
    result_dict['tpm'] = 0.0 if game.three_pointers is None else game.three_pointers
    result_dict['to'] = 0.0 if game.turnovers is None else game.turnovers
    result_dict['fgm'] = 0.0 if game.field_goals is None else game.field_goals
    result_dict['fga'] = 0.0 if game.field_goal_attempts is None else game.field_goal_attempts
    result_dict['ftm'] = 0.0 if game.free_throws is None else game.free_throws
    result_dict['fta'] = 0.0 if game.free_throw_attempts is None else game.free_throw_attempts
    return result_dict




def play_matchup(team_a, team_b, elo_data, player_games):
    """Simulates a matchup between two teams and updates ELOs."""
    team_a_stats = defaultdict(float)
    team_b_stats = defaultdict(float)
    
    # Aggregate stats for Team A
    for player in team_a:
        # players play 3 or 4 games per week
        num_games = random.randint(3,4)
        for i in range(num_games):
            game = get_game(player.id, player_games)
            if game is None: continue
            team_a_stats['pts'] += game['points']
            team_a_stats['reb'] += game['reb']
            team_a_stats['ast'] += game['ast']
            team_a_stats['stl'] += game['stl']
            team_a_stats['blk'] += game['blk']
            team_a_stats['tpm'] += game['tpm']
            team_a_stats['to'] += game['to']
            team_a_stats['fgm'] += game['fgm']
            team_a_stats['fga'] += game['fga']
            team_a_stats['ftm'] += game['ftm']
            team_a_stats['fta'] += game['fta']
        
    # Aggregate stats for Team B
    for player in team_b:
        # players play 3 or 4 games per week
        num_games = random.randint(3,4)
        for i in range(num_games):
            game = get_game(player.id, player_games)
            if game is None: continue
            team_b_stats['pts'] += game['points']
            team_b_stats['reb'] += game['reb']
            team_b_stats['ast'] += game['ast']
            team_b_stats['stl'] += game['stl']
            team_b_stats['blk'] += game['blk']
            team_b_stats['tpm'] += game['tpm']
            team_b_stats['to'] += game['to']
            team_b_stats['fgm'] += game['fgm']
            team_b_stats['fga'] += game['fga']
            team_b_stats['ftm'] += game['ftm']
            team_b_stats['fta'] += game['fta']

    # Calculate percentages
    team_a_stats['fg_pct'] = team_a_stats['fgm'] / team_a_stats['fga'] if team_a_stats['fga'] else 0
    team_a_stats['ft_pct'] = team_a_stats['ftm'] / team_a_stats['fta'] if team_a_stats['fta'] else 0
    team_b_stats['fg_pct'] = team_b_stats['fgm'] / team_b_stats['fga'] if team_b_stats['fga'] else 0
    team_b_stats['ft_pct'] = team_b_stats['ftm'] / team_b_stats['fta'] if team_b_stats['fta'] else 0

    # Determine winners for each category
    team_a_score = 0
    team_b_score = 0

    for cat_name, elo_key in CATEGORIES.items():
        # For turnovers, lower is better
        if cat_name == 'to':
            if team_a_stats[cat_name] < team_b_stats[cat_name]:
                update_elos(team_a, team_b, elo_data, elo_key)
                team_a_score += 1
            elif team_b_stats[cat_name] < team_a_stats[cat_name]:
                update_elos(team_b, team_a, elo_data, elo_key)
                team_b_score += 1
        else:
            if team_a_stats[cat_name] > team_b_stats[cat_name]:
                update_elos(team_a, team_b, elo_data, elo_key)
                team_a_score += 1
            elif team_b_stats[cat_name] > team_a_stats[cat_name]:
                update_elos(team_b, team_a, elo_data, elo_key)
                team_b_score += 1

    # Update overall ELO based on the final score
    if team_a_score > team_b_score:
        update_elos(team_a, team_b, elo_data, 'overall_elo')
    elif team_b_score > team_a_score:
        update_elos(team_b, team_a, elo_data, 'overall_elo')

def run_simulations():
    """Main function to orchestrate the fantasy basketball ELO simulation."""
    db = SessionLocal()
    try:
        print("Loading player and game data from the database...")
        # Eagerly load game_stats to avoid N+1 query problems later
        all_players = db.query(Player).options(joinedload(Player.game_stats)).all()
        
        # Filter out players with no game stats to avoid errors
        players_with_games = [p for p in all_players if p.game_stats]
        
        if not players_with_games:
            print("No players with game stats found in the database. Exiting.")
            return

        print(f"{len(players_with_games)} players with game data loaded.")
        
        # Create a dictionary for quick access to a player's games
        player_games = {p.id: p.game_stats for p in players_with_games}

        # --- Initialize ELO Ratings ---
        existing_elos = {elo.player_id: elo for elo in db.query(EloStats).all()}
        elo_data = {}
        
        for player in players_with_games:
            if player.id in existing_elos:
                p_elo = existing_elos[player.id]
                elo_data[player.id] = {
                    'overall_elo': p_elo.overall_elo,
                    'fg_pct_elo': p_elo.fg_pct_elo,
                    'ft_pct_elo': p_elo.ft_pct_elo,
                    'pts_elo': p_elo.pts_elo,
                    'reb_elo': p_elo.reb_elo,
                    'ast_elo': p_elo.ast_elo,
                    'stl_elo': p_elo.stl_elo,
                    'blk_elo': p_elo.blk_elo,
                    'to_elo': p_elo.to_elo,
                    'tpm_elo': p_elo.tpm_elo,
                }
            else: # If player has no ELO entry yet, create one
                elo_data[player.id] = {key: INITIAL_ELO for key in CATEGORIES.values()}
                elo_data[player.id]['overall_elo'] = INITIAL_ELO

        # --- Main Simulation Loop ---
        for i in range(NUM_SIMULATIONS):
            print(f"Running simulation {i + 1}/{NUM_SIMULATIONS}...")
            
            # Sort players by overall ELO for drafting
            # TODO, is this correct? some players may never get selected
            available_players = sorted(
                players_with_games, 
                key=lambda p: elo_data.get(p.id, {}).get('overall_elo', INITIAL_ELO),
                reverse=True
            )
            
            # Draft teams
            teams = [[] for _ in range(NUM_TEAMS)]
            for round_num in range(TEAM_SIZE):
                for team_idx in range(NUM_TEAMS):
                    if available_players:
                        # For now, pick a random player, maybe change this later
                        player_to_draft = random.choice(available_players)
                        available_players.remove(player_to_draft)
                        # player_to_draft = available_players.pop(0)
                        teams[team_idx].append(player_to_draft)

            # Play a full round-robin set of matchups
            matchups = itertools.combinations(teams, 2)
            for team_a, team_b in matchups:
                play_matchup(team_a, team_b, elo_data, player_games)

        # --- Update Database After All Simulations ---
        print("\nAll simulations complete. Updating the database...")
        
        for player_id, ratings in elo_data.items():
            elo_record = db.query(EloStats).filter(EloStats.player_id == player_id).first()
            if not elo_record:
                elo_record = EloStats(player_id=player_id)
                db.add(elo_record)
            
            # Assign updated values
            elo_record.overall_elo = ratings['overall_elo']
            elo_record.fg_pct_elo = ratings['fg_pct_elo']
            elo_record.ft_pct_elo = ratings['ft_pct_elo']
            elo_record.pts_elo = ratings['pts_elo']
            elo_record.reb_elo = ratings['reb_elo']
            elo_record.ast_elo = ratings['ast_elo']
            elo_record.stl_elo = ratings['stl_elo']
            elo_record.blk_elo = ratings['blk_elo']
            elo_record.to_elo = ratings['to_elo']
            elo_record.tpm_elo = ratings['tpm_elo']

        # Update simulation count
        sim_info = db.query(SimulationInfo).first()
        if not sim_info:
            sim_info = SimulationInfo(simulation_count=NUM_SIMULATIONS)
            db.add(sim_info)
        else:
            sim_info.simulation_count += NUM_SIMULATIONS
        
        db.commit()
        print("Database has been successfully updated with new ELO ratings.")

    finally:
        db.close()

if __name__ == "__main__":
    run_simulations()
