"""
League Roster Importer Library & Free Agent Finder

1. Reads 'teams_and_players.csv' to map players to DB IDs.
2. Provides clean rosters for analysis (excluding DEAD).
3. Identifies legitimate Free Agents (excluding DEAD and Active Rosters).
"""

import csv
from typing import Dict, List, Tuple, Optional
from sqlalchemy import func

# Imports from your database file
try:
    from database import SessionLocal, Player, GameStats
except ImportError:
    print("FATAL ERROR: 'database.py' not found.")
    print("Please ensure 'database.py' is in the same directory.")
    exit(1)

ROSTER_CSV_FILE = 'teams_and_players.csv'
FUZZY_MATCH_THRESHOLD = 5 

def _levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculates the Levenshtein (edit) distance between two strings.
    """
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def _read_roster_csv() -> Dict[str, List[Tuple[str, Optional[str]]]]:
    """
    INTERNAL HELPER: Reads the CSV and returns a dictionary of ALL teams,
    including the 'DEAD' team. 
    
    Used by both public functions to ensure consistent data loading.
    """
    print(f"--- Reading {ROSTER_CSV_FILE} ---")
    session = SessionLocal()
    
    # 1. Build the player map from the database
    name_to_id_map = {}
    try:
        all_players = session.query(Player.name, Player.player_id).all()
        for p in all_players:
            if p.name and p.player_id:
                name_to_id_map[p.name] = p.player_id
    except Exception as e:
        print(f"FATAL ERROR: Could not query Player table: {e}")
        session.close()
        return {}
    
    # 2. Process the CSV and perform matching
    raw_rosters_map = {}
    
    try:
        with open(ROSTER_CSV_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            # Handle potential header
            header = next(reader, None)
            if header and "Team" not in header[0] and "Alex" in header[0]:
                 f.seek(0)
            
            for row in reader:
                if not row or len(row) < 2:
                    continue
                    
                team = row[0].strip()
                csv_player_name = row[1].strip()
                status = row[2].strip() if len(row) > 2 and row[2] else None

                if team == "Team" and csv_player_name == "Player":
                    continue

                if not csv_player_name:
                    continue

                player_id_string = None
                
                # Exact Match
                if csv_player_name in name_to_id_map:
                    player_id_string = name_to_id_map[csv_player_name]
                else:
                    # Fuzzy Match
                    min_dist = float('inf')
                    best_match_name = None
                    
                    for db_name in name_to_id_map.keys():
                        dist = _levenshtein_distance(csv_player_name, db_name)
                        if dist < min_dist:
                            min_dist = dist
                            best_match_name = db_name
                    
                    if best_match_name and min_dist <= FUZZY_MATCH_THRESHOLD:
                        player_id_string = name_to_id_map[best_match_name]
                        # Optional: print(f"LOG: Fuzzy match '{csv_player_name}' -> '{best_match_name}'")
                    else:
                        print(f"ERROR: Failed to find any match for '{csv_player_name}'. Skipping.")
                        continue

                if team not in raw_rosters_map:
                    raw_rosters_map[team] = []
                
                raw_rosters_map[team].append( (player_id_string, status) )

    except FileNotFoundError:
        print(f"FATAL ERROR: '{ROSTER_CSV_FILE}' not found.")
        session.close()
        return {}
    except Exception as e:
        print(f"An error occurred while reading CSV: {e}")
        session.close()
        return {}

    session.close()
    return raw_rosters_map


def get_league_rosters() -> Dict[str, List[Tuple[str, Optional[str]]]]:
    """
    Public function to get active league rosters.
    
    IMPORTANT: This EXCLUDES the 'DEAD' team to prevent analysis issues.
    """
    # Get everything
    all_rosters = _read_roster_csv()
    
    # Remove DEAD team if it exists
    if "DEAD" in all_rosters:
        del all_rosters["DEAD"]
        
    print(f"--- Roster Import Complete (DEAD team excluded) ---")
    return all_rosters


def get_available_free_agents() -> List[str]:
    """
    Finds all players in the database who have played at least one game
    in 2025 or 2026.
    
    It REMOVES:
    1. Players on active rosters.
    2. Players on the 'DEAD' team.
    
    Returns:
        List[str]: A list of player_ids (e.g. ['jamesle01', ...])
    """
    print("--- Identifying Available Free Agents ---")
    
    # 1. Get ALL rostered players (including DEAD) to ensure we don't pick them
    all_rosters = _read_roster_csv() # Calls the helper directly to get DEAD team too
    
    unavailable_players = set()
    for team, players in all_rosters.items():
        for player_id, status in players:
            if player_id:
                unavailable_players.add(player_id)

    print(f"Found {len(unavailable_players)} unavailable players (Active + DEAD).")

    # 2. Query DB for players active in 2025/2026
    session = SessionLocal()
    available_free_agents = []
    
    try:
        # Query GameStats for seasons 2025/2026
        query = (
            session.query(Player.player_id)
            .join(GameStats)
            .filter(GameStats.season.in_([2025, 2026]))
            .distinct()
        )
        
        active_db_results = query.all()
        
        # 3. Filter out unavailable players
        for row in active_db_results:
            pid = row[0]
            if pid and pid not in unavailable_players:
                available_free_agents.append(pid)
                
        print(f"Total active players in DB (2025/26): {len(active_db_results)}")
        print(f"Total Free Agents (Active DB - Unavailable): {len(available_free_agents)}")
        
    except Exception as e:
        print(f"Error querying free agents: {e}")
    finally:
        session.close()

    return available_free_agents