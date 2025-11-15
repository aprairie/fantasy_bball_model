"""
Executable script to run the 'get_league_rosters' function
and print the results.
"""

import pprint

try:
    from teams_and_players_lib import get_league_rosters
except ImportError:
    print("FATAL ERROR: 'league_rosters_lib.py' not found.")
    print("Please ensure that file is in the same directory.")
    exit(1)


def main():
    """
    Main executable function.
    """
    rosters = get_league_rosters()
    
    if not rosters:
        print("\nNo rosters were loaded.")
        return

    print("\n--- Final Roster Map ---")
    pprint.pprint(rosters)
    print("\nDone.")


if __name__ == "__main__":
    # This block ensures 'init_db' is called if this script
    # is run directly, which might be needed to register models
    # before the library's SessionLocal() is used.
    try:
        from database import init_db
        print("Initializing DB (if needed)...")
        init_db()
    except ImportError:
        print("Could not import init_db. Assuming DB is ready.")
        
    main()