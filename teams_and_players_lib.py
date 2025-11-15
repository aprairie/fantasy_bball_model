"""
League Roster Importer Library

This library reads the 'league_rosters.csv' file, connects to the
database to get a map of all players, and then uses fuzzy matching
to map the CSV player names to the database player IDs.
"""

import csv
from typing import Dict, List, Tuple, Optional

# Imports from your database file
try:
    from database import SessionLocal, Player
except ImportError:
    print("FATAL ERROR: 'database.py' not found.")
    print("Please ensure 'database.py' is in the same directory.")
    exit(1)

# File to read from
ROSTER_CSV_FILE = 'teams_and_players.csv'
# Edit distance threshold. If the best match is worse than this,
# it's probably not the right player. (We still take it, but a high
# distance log might indicate a bad typo).
FUZZY_MATCH_THRESHOLD = 5 

def _levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculates the Levenshtein (edit) distance between two strings.
    A simple, non-optimized implementation.
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


def get_league_rosters() -> Dict[str, List[Tuple[str, Optional[str]]]]:
    """
    Reads 'league_rosters.csv' and maps players to DB player IDs.

    Returns:
        A dictionary of:
        { team_name: [(player_id_string, status), ...] }
    """
    print("--- Starting Roster Import ---")
    session = SessionLocal()
    
    # 1. Build the player map from the database
    # { "Derrick White": "whitede01", "Paul George": "georgpa01", ... }
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
    
    print(f"Loaded {len(name_to_id_map)} players from database.")
    
    # 2. Process the CSV and perform matching
    league_rosters_map = {}
    
    try:
        with open(ROSTER_CSV_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            # Skip header row
            try:
                next(reader)
            except StopIteration:
                print("CSV file is empty.")
                return {}

            for row in reader:
                if not row or len(row) < 2:
                    continue
                    
                team = row[0]
                csv_player_name = row[1].strip()
                status = row[2].strip() if len(row) > 2 and row[2] else None

                if not csv_player_name:
                    continue

                player_id_string = None
                
                # Step 2a: Try for an exact match
                if csv_player_name in name_to_id_map:
                    player_id_string = name_to_id_map[csv_player_name]
                else:
                    # Step 2b: No exact match, run fuzzy matching
                    min_dist = float('inf')
                    best_match_name = None
                    
                    for db_name in name_to_id_map.keys():
                        dist = _levenshtein_distance(csv_player_name, db_name)
                        if dist < min_dist:
                            min_dist = dist
                            best_match_name = db_name
                    
                    if best_match_name:
                        player_id_string = name_to_id_map[best_match_name]
                        # This is the log line you requested
                        print(
                            f"LOG: No exact match for '{csv_player_name}'. "
                            f"Best match (dist={min_dist}): '{best_match_name}' "
                            f"(ID: {player_id_string})"
                        )
                    else:
                        print(f"ERROR: Failed to find any match for '{csv_player_name}'. Skipping.")
                        continue

                # Step 2c: Add the matched player to the final map
                if team not in league_rosters_map:
                    league_rosters_map[team] = []
                
                league_rosters_map[team].append( (player_id_string, status) )

    except FileNotFoundError:
        print(f"FATAL ERROR: '{ROSTER_CSV_FILE}' not found.")
        session.close()
        return {}
    except Exception as e:
        print(f"An error occurred while reading CSV: {e}")
        session.close()
        return {}

    session.close()
    print("--- Roster Import Complete ---")
    return league_rosters_map