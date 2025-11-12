import csv
from sqlalchemy.orm import Session
from sqlalchemy import desc
import os

try:
    # Import your database models and session
    from database import SessionLocal, Player, PlayerSeasonValue
except ImportError:
    print("Error: Could not import from database.py.")
    print("Please make sure this script is in the same directory as database.py.")
    exit(1)

# --- Configuration ---
TARGET_SEASON = 2026
OUTPUT_FILENAME = f"player_ranks_{TARGET_SEASON}.csv"
# Get the directory of the current script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Place the CSV in the same directory
OUTPUT_PATH = os.path.join(SCRIPT_DIR, OUTPUT_FILENAME)

def export_ranks_to_csv():
    """
    Fetches player season values and exports them to a CSV file.
    """
    print(f"--- Starting export for season {TARGET_SEASON} ---")
    db: Session = SessionLocal()
    
    try:
        # This is the SQLAlchemy equivalent of your SQL query
        results = (
            db.query(Player.name, PlayerSeasonValue)
            .join(Player, PlayerSeasonValue.player_id == Player.id)
            .filter(PlayerSeasonValue.season == TARGET_SEASON)
            .order_by(desc(PlayerSeasonValue.total_score))
            .all()
        )
        
        if not results:
            print(f"No data found for season {TARGET_SEASON}. No file will be created.")
            return

        print(f"Found {len(results)} records. Writing to {OUTPUT_PATH}...")
        
        # We need to manually construct the headers
        # Get headers from the PlayerSeasonValue model
        model_headers = [col.name for col in PlayerSeasonValue.__table__.columns]
        # Add the 'name' from the Player model at the beginning
        headers = ["name"] + model_headers

        with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write the header row
            writer.writerow(headers)
            
            # Write the data rows
            for name, season_data in results:
                # Create the row in the correct order
                row = [name] + [getattr(season_data, col) for col in model_headers]
                writer.writerow(row)
                
        print(f"\nSuccessfully saved data to {OUTPUT_PATH}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        db.close()
        print("--- Database session closed. ---")

if __name__ == "__main__":
    export_ranks_to_csv()