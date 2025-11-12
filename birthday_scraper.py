import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
from sqlalchemy.orm import Session
from sqlalchemy import exc
from typing import Union

# Import the SessionLocal and Player model from your database.py file
try:
    from database import SessionLocal, Player
except ImportError:
    print("Error: Could not import SessionLocal or Player from database.py.")
    print("Please make sure this script is in the same directory as database.py.")
    exit(1)

#                       Changed this line v
def get_bbr_birth_date(player_id: str) -> Union[datetime.date, None]:
    """
    Fetches a player's page on Basketball-Reference and scrapes their birth date.
    
    The birth date is stored in a <span id="necro-birth"> tag,
    specifically in the 'data-birth' attribute (YYYY-MM-DD).
    """
    if not player_id:
        return None
    
    # Construct the URL. Example: /players/j/jamesle01.html
    letter = player_id[0]
    url = f"https://www.basketball-reference.com/players/{letter}/{player_id}.html"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        # Make the request
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes (404, 500, etc.)
        
        # Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the specific span
        birth_span = soup.find('span', id='necro-birth')
        
        if birth_span and 'data-birth' in birth_span.attrs:
            date_str = birth_span['data-birth']
            # Convert 'YYYY-MM-DD' string to a date object
            birth_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            return birth_date
        else:
            print(f"  [!] Could not find 'data-birth' info for {player_id}.")
            return None
            
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"  [!] Player page not found (404) for {player_id} at {url}")
        else:
            print(f"  [!] HTTP Error for {player_id}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"  [!] Network error for {player_id}: {e}")
    except Exception as e:
        print(f"  [!] An unexpected error occurred for {player_id}: {e}")
        
    return None

def main():
    print("--- Starting Player Birth Date Scraper ---")
    db: Session = SessionLocal()
    
    try:
        # Find players who have a bball-ref ID but are missing a birth date
        players_to_update = db.query(Player).filter(
            Player.player_id != None,
            Player.birth_date == None
        ).all()
        
        if not players_to_update:
            print("No players found needing a birth date update. Database is up-to-date.")
            return

        print(f"Found {len(players_to_update)} players to update.")
        
        updated_count = 0
        batch_size = 25 # Commit changes in batches

        for i, player in enumerate(players_to_update):
            print(f"Processing {i+1}/{len(players_to_update)}: {player.name} ({player.player_id})")
            
            # --- POLITENESS: Wait between requests ---
            # Be respectful to the website's servers.
            time.sleep(3.0) 
            
            birth_date = get_bbr_birth_date(player.player_id)
            
            if birth_date:
                player.birth_date = birth_date
                player.updated_at = datetime.utcnow() # Also update the 'updated_at' timestamp
                updated_count += 1
                print(f"  [+] Found: {birth_date}")
            
            # Commit in batches to save progress
            if (updated_count > 0 and updated_count % batch_size == 0) or (i == len(players_to_update) - 1):
                print(f"\n--- Committing batch of {updated_count} updates to database... ---")
                try:
                    db.commit()
                    print("--- Batch committed successfully. ---")
                    updated_count = 0 # Reset batch counter
                except exc.SQLAlchemyError as e:
                    print(f"[!!!] Database commit failed: {e}")
                    db.rollback()
                    print("--- Rolled back changes for this batch. ---")

        print("\n--- Scraping and updating complete. ---")
        
    except Exception as e:
        print(f"An error occurred during the main process: {e}")
    finally:
        db.close()
        print("--- Database session closed. ---")

if __name__ == "__main__":
    main()