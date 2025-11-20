import csv
import sys
import os
from sqlalchemy.orm import Session
from database import Player, PlayerSeasonValue

# NEW: Import roster function
try:
    from teams_and_players_lib import get_league_rosters
except ImportError:
    print("Warning: Could not import 'get_league_rosters'. Fantasy Team column will be empty.", file=sys.stderr)
    def get_league_rosters(): return {}

# Mapping for specific stat columns
STAT_COLS = [
    ('z_pts', 'pts_score'),
    ('z_reb', 'reb_score'),
    ('z_ast', 'ast_score'),
    ('z_stl', 'stl_score'),
    ('z_blk', 'blk_score'),
    ('z_tpm', 'tpm_score'),
    ('z_fg', 'fg_pct_score'),
    ('z_ft', 'ft_pct_score'),
    ('z_to', 'to_score')
]

def export_player_stats_to_csv(session: Session, filename="output/player_stats.csv"):
    """
    Exports player stats directly to a CSV file in the output directory.
    Now includes the Fantasy Team owner for each player.
    """
    # --- SAFETY CHECK ---
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
    except OSError as e:
        print(f"Error creating directory: {e}", file=sys.stderr)
        return

    print(f"Starting export to {filename}...")
    
    # 1. Load and Index Roster Data
    # Convert {Team: [(pid, status), ...]} -> {pid: Team}
    rosters = get_league_rosters()
    player_to_team_map = {}
    
    for team_name, player_list in rosters.items():
        for pid, status in player_list:
            # We map the string ID (e.g. 'curryst01') to the team name
            player_to_team_map[pid] = team_name

    # 2. Fetch all players who have stats in 2025 OR 2026
    relevant_ids = [
        r[0] for r in session.query(PlayerSeasonValue.player_id)
        .filter(PlayerSeasonValue.season.in_([2025, 2026]))
        .distinct()
        .all()
    ]
    
    if not relevant_ids:
        print("No players found with data in 2025 or 2026.", file=sys.stderr)
        return

    # Fetch players
    players = (
        session.query(Player)
        .filter(Player.id.in_(relevant_ids))
        .all()
    )

    # 3. Define CSV Headers (Added 'fantasy_team')
    headers = [
        "player_id", "player_name", "fantasy_team",  # <--- NEW COLUMN
        "val_2023", "val_2024", "val_2025", "val_2026", "val_1", "val_2"
    ]
    
    for slug, _ in STAT_COLS:
        headers.append(f"{slug}_2026")
    for slug, _ in STAT_COLS:
        headers.append(f"{slug}_1")
        
    headers.append("play_likelihood_1")

    # 4. Write to File
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)

            for p in players:
                sv_map = {sv.season: sv for sv in p.season_values}
                
                # Lookup Fantasy Team (default to "Free Agent" or empty if not found)
                # p.player_id is the string ID (e.g. 'jamesle01')
                fantasy_team = player_to_team_map.get(p.player_id, "")

                row = [
                    p.player_id,
                    p.name,
                    fantasy_team,  # <--- NEW DATA
                    sv_map[2023].total_score if 2023 in sv_map else "",
                    sv_map[2024].total_score if 2024 in sv_map else "",
                    sv_map[2025].total_score if 2025 in sv_map else "",
                    sv_map[2026].total_score if 2026 in sv_map else "",
                    sv_map[1].total_score if 1 in sv_map else "",
                    sv_map[2].total_score if 2 in sv_map else "",
                ]

                # Add 2026 Z-scores
                if 2026 in sv_map:
                    obj = sv_map[2026]
                    for _, attr in STAT_COLS:
                        row.append(getattr(obj, attr))
                else:
                    row.extend([""] * 9)

                # Add Year 1 Z-scores
                if 1 in sv_map:
                    obj = sv_map[1]
                    for _, attr in STAT_COLS:
                        row.append(getattr(obj, attr))
                else:
                    row.extend([""] * 9)

                # Add Play Likelihood
                if 1 in sv_map:
                    row.append(sv_map[1].play_likelihood)
                else:
                    row.append("")

                writer.writerow(row)
                
        print(f"Successfully wrote {len(players)} rows to {filename}")
        
    except IOError as e:
        print(f"Error writing to file: {e}", file=sys.stderr)