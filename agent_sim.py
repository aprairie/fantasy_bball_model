import random
import itertools
from collections import defaultdict
from sqlalchemy.orm import joinedload
from database import SessionLocal, Player, EloStats
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Simulation Constants ---
NUM_SIMULATIONS = 10000
NUM_TEAMS = 10
TEAM_SIZE = 13
INITIAL_AGENT_ELO = 1500.0
K_FACTOR = 20  # A standard K-factor for agent-vs-agent Elo

# Category keys used for evaluation
ELO_KEYS = [
    'pts_elo', 'reb_elo', 'ast_elo', 'stl_elo', 'blk_elo',
    'tpm_elo', 'to_elo', 'fg_pct_elo', 'ft_pct_elo'
]

# --- Core Simulation Functions (Adapted from model.py) ---

def get_random_game_stats(player, player_games_cache):
    """Selects a single random game from a player's history."""
    if not player or not player_games_cache.get(player.id):
        return None
    game = random.choice(player_games_cache[player.id])
    return {
        'pts': game.points or 0.0, 'reb': game.total_rebounds or 0.0,
        'ast': game.assists or 0.0, 'stl': game.steals or 0.0,
        'blk': game.blocks or 0.0, 'tpm': game.three_pointers or 0.0,
        'to': game.turnovers or 0.0, 'fgm': game.field_goals or 0.0,
        'fga': game.field_goal_attempts or 0.0, 'ftm': game.free_throws or 0.0,
        'fta': game.free_throw_attempts or 0.0,
    }

def aggregate_team_stats(team, player_games_cache):
    """Aggregates the stats for an entire team for a weekly matchup."""
    team_stats = defaultdict(float)
    for player in team:
        num_games = random.randint(3, 4)
        for _ in range(num_games):
            game_stats = get_random_game_stats(player, player_games_cache)
            if game_stats:
                for stat, value in game_stats.items():
                    team_stats[stat] += value
    
    team_stats['fg_pct'] = team_stats['fgm'] / team_stats['fga'] if team_stats['fga'] else 0
    team_stats['ft_pct'] = team_stats['ftm'] / team_stats['fta'] if team_stats['fta'] else 0
    return team_stats

def calculate_expected_outcome(agent_elo, opponent_elo):
    """Calculates the probability of an agent winning based on Elo ratings."""
    return 1 / (1 + 10 ** ((opponent_elo - agent_elo) / 400))

def update_agent_elos(winner_agent, loser_agent):
    """Updates the Elo ratings for the winning and losing agents."""
    expected_win_prob = calculate_expected_outcome(winner_agent.elo, loser_agent.elo)
    elo_change = K_FACTOR * (1 - expected_win_prob)
    winner_agent.elo += elo_change
    loser_agent.elo -= elo_change

# --- Agent and Strategy Classes ---

class Agent:
    """Base class for a fantasy draft agent."""
    def __init__(self, agent_id, name, strategy_description):
        self.id = agent_id
        self.name = name
        self.strategy = strategy_description
        self.elo = INITIAL_AGENT_ELO
        self.team = []

    def select_player(self, available_players):
        # This method should be overridden by subclasses
        raise NotImplementedError

    def reset(self):
        self.team = []

class BestPlayerAgent(Agent):
    """Selects the player with the highest overall_elo."""
    def select_player(self, available_players):
        return max(available_players, key=lambda p: p.elo_stats.overall_elo)

class PuntCategoryAgent(Agent):
    """Ignores one or more categories when evaluating players."""
    def __init__(self, agent_id, name, strategy_description, punt_categories):
        super().__init__(agent_id, name, strategy_description)
        self.punt_categories = punt_categories
        self.relevant_elos = [k for k in ELO_KEYS if k not in self.punt_categories]

    def _calculate_player_score(self, player):
        return sum(getattr(player.elo_stats, key) for key in self.relevant_elos)

    def select_player(self, available_players):
        return max(available_players, key=self._calculate_player_score)

class AdaptivePuntAgent(Agent):
    """Picks BPA for 3 rounds, then punts its weakest category."""
    def __init__(self, agent_id, name, strategy_description):
        super().__init__(agent_id, name, strategy_description)
        self.punt_category = None

    def select_player(self, available_players):
        if len(self.team) < 3: # Rounds 1-3
            return max(available_players, key=lambda p: p.elo_stats.overall_elo)
        else: # Round 4 onwards
            if not self.punt_category:
                self._determine_punt_category()
            
            relevant_elos = [k for k in ELO_KEYS if k != self.punt_category]
            return max(available_players, key=lambda p: sum(getattr(p.elo_stats, key) for key in relevant_elos))

    def _determine_punt_category(self):
        team_cat_elos = defaultdict(float)
        for player in self.team:
            for key in ELO_KEYS:
                team_cat_elos[key] += getattr(player.elo_stats, key)
        
        # For turnovers, a lower Elo sum is better, so we find the max to punt it.
        # This logic is a bit inverted, a simpler way is to find the minimum of others.
        non_to_keys = [k for k in ELO_KEYS if k != 'to_elo']
        self.punt_category = min(non_to_keys, key=lambda k: team_cat_elos[k])
        logger.debug(f"{self.name} identified '{self.punt_category}' as its weakest category.")

    def reset(self):
        super().reset()
        self.punt_category = None

class BuildFocusAgent(Agent):
    """Focuses on a specific build (e.g., 'Big Man' or 'Guard')."""
    def __init__(self, agent_id, name, strategy_description, focus_categories):
        super().__init__(agent_id, name, strategy_description)
        self.focus_categories = focus_categories

    def _calculate_player_score(self, player):
        return sum(getattr(player.elo_stats, key) for key in self.focus_categories) / len(self.focus_categories)

    def select_player(self, available_players):
        return max(available_players, key=self._calculate_player_score)
        
class BalancedBuildAgent(Agent):
    """Identifies its weakest category at each pick and drafts to improve it."""
    def select_player(self, available_players):
        if not self.team:
            return max(available_players, key=lambda p: p.elo_stats.overall_elo)

        team_cat_elos = defaultdict(float)
        for player in self.team:
            for key in ELO_KEYS:
                team_cat_elos[key] += getattr(player.elo_stats, key)
        
        non_to_keys = [k for k in ELO_KEYS if k != 'to_elo']
        weakest_category = min(non_to_keys, key=lambda k: team_cat_elos[k])

        # Find the player who is best in that specific weakest category
        return max(available_players, key=lambda p: getattr(p.elo_stats, weakest_category))

# --- Main Simulation Orchestration ---

def run_draft_simulations():
    """Main function to simulate fantasy drafts and seasons for different agent strategies."""
    db = SessionLocal()
    try:
        logger.info("Loading all players and their Elo stats from the database...")
        # Load players with their Elo stats eagerly to avoid N+1 queries
        all_players = db.query(Player).options(joinedload(Player.elo_stats)).join(EloStats).all()
        # Filter for players who actually have Elo stats
        draftable_players = [p for p in all_players if p.elo_stats is not None]
        
        # Cache game stats to speed up lookups inside the simulation loop
        logger.info("Caching player game stats...")
        player_games_cache = {p.id: p.game_stats for p in draftable_players}

        logger.info(f"Starting simulation with {len(draftable_players)} draftable players.")

        # --- Initialize Agents ---
        agents = [
            BestPlayerAgent(1, "BPA Agent", "Picks best player available by overall_elo"),
            PuntCategoryAgent(2, "Punt FT% Agent", "Ignores ft_pct_elo", ['ft_pct_elo']),
            AdaptivePuntAgent(3, "Adaptive Punt Agent", "BPA for 3 rounds, then punts worst category"),
            PuntCategoryAgent(4, "Punt Points Agent", "Ignores pts_elo", ['pts_elo']),
            PuntCategoryAgent(5, "Punt Assists Agent", "Ignores ast_elo", ['ast_elo']),
            PuntCategoryAgent(6, "Punt Turnovers Agent", "Ignores to_elo", ['to_elo']),
            BuildFocusAgent(7, "Big Man Build", "Focuses on REB, BLK, FG%", ['reb_elo', 'blk_elo', 'fg_pct_elo']),
            BuildFocusAgent(8, "Guard Build", "Focuses on PTS, AST, STL, TPM, FT%", ['pts_elo', 'ast_elo', 'stl_elo', 'tpm_elo', 'ft_pct_elo']),
            BalancedBuildAgent(9, "Balanced Agent", "Drafts to fix weakest category"),
            PuntCategoryAgent(10, "Punt FG%/REB Agent", "Ignores fg_pct_elo and reb_elo", ['fg_pct_elo', 'reb_elo']),
        ]

        # --- Main Simulation Loop ---
        for i in range(NUM_SIMULATIONS):
            # Reset agents and available players for a new season
            available_players = list(draftable_players)
            for agent in agents:
                agent.reset()

            # --- Draft Phase ---
            for round_num in range(TEAM_SIZE):
                team_order = range(NUM_TEAMS) if round_num % 2 == 0 else reversed(range(NUM_TEAMS))
                for team_idx in team_order:
                    agent = agents[team_idx]
                    if available_players:
                        chosen_player = agent.select_player(available_players)
                        agent.team.append(chosen_player)
                        available_players.remove(chosen_player)

            # --- Season Phase (Round-Robin) ---
            matchups = itertools.combinations(agents, 2)
            for agent_a, agent_b in matchups:
                stats_a = aggregate_team_stats(agent_a.team, player_games_cache)
                stats_b = aggregate_team_stats(agent_b.team, player_games_cache)
                
                score_a, score_b = 0, 0
                for cat in ['pts', 'reb', 'ast', 'stl', 'blk', 'tpm', 'fg_pct', 'ft_pct']:
                    if stats_a[cat] > stats_b[cat]: score_a += 1
                    elif stats_b[cat] > stats_a[cat]: score_b += 1
                
                # Turnovers are won by the lower score
                if stats_a['to'] < stats_b['to']: score_a += 1
                elif stats_b['to'] < stats_a['to']: score_b += 1

                if score_a > score_b:
                    update_agent_elos(agent_a, agent_b)
                elif score_b > score_a:
                    update_agent_elos(agent_b, agent_a)
            
            if (i + 1) % 500 == 0:
                logger.info(f"Completed simulation {i + 1}/{NUM_SIMULATIONS}")

        # --- Final Results ---
        logger.info("\n--- All simulations complete. Final Agent Elo Ratings ---")
        sorted_agents = sorted(agents, key=lambda a: a.elo, reverse=True)
        
        print("\n" + "="*60)
        print(" " * 15 + "Final Agent Elo Rankings")
        print("="*60)
        for rank, agent in enumerate(sorted_agents, 1):
            print(f" #{rank:<2} | {agent.name:<20} | Elo: {agent.elo:<8.2f} | Strategy: {agent.strategy}")
        print("="*60)

    finally:
        db.close()

if __name__ == "__main__":
    run_draft_simulations()
