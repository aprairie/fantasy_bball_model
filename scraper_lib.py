import requests
from bs4 import BeautifulSoup
import time
import logging
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import exc
from typing import Union, List, Dict, Any

# Import database models
try:
    from database import Player, GameStats
except ImportError:
    print("Error: Could not import Player or GameStats from database.py.")
    exit(1)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BasketballScraper:
    """
    A unified scraper for fetching player game logs and birth dates from
    Basketball-Reference.com.
    """
    def __init__(self):
        self.base_url = "https://www.basketball-reference.com"
        self.headers = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/91.0.4472.124 Safari/537.36'
            )
        }

    # --- Public Methods (Main entry points) ---

    def scrape_game_logs_for_season(self, session: Session, year: int):
        """
        Scrapes all player game logs for a given season and saves them to the DB.
        """
        logger.info(f"--- Starting scrape for {year} season... ---")

        players = self._get_active_players_list(year)
        
        if not players:
            logger.error(f"No players found for {year} season.")
            return

        logger.info(f"Will scrape game logs for {len(players)} players for {year}...")

        for i, player_info in enumerate(players, 1):
            
            games_data = self._scrape_player_game_log(
                player_info['player_id'],
                player_info['name'],
                year
            )
            
            total_scraped_games = len(games_data)
            new_games_saved = 0
            
            if games_data:
                try:
                    new_games_saved = self._save_games_to_db(
                        session, player_info, games_data
                    )
                except exc.SQLAlchemyError as e:
                    logger.error(f"Database error for {player_info['name']}: {e}")
                    logger.warning("Rolling back session...")
                    session.rollback()
                    logger.warning("Session rolled back. Skipping this player.")
                    continue # Skip this player
            
            logger.info(
                f"({year}) {player_info['name']} [{i}/{len(players)}]: "
                f"Scraped {total_scraped_games} games. {new_games_saved} new games saved."
            )
            
            # Politeness: wait between scraping each player
            if i < len(players):
                time.sleep(3.0)
        
        logger.info(f"--- Scraping for {year} season completed! ---")

    def scrape_all_player_birthdays(self, session: Session):
        """
        Finds all players in the DB missing a birth date and scrapes it.
        """
        logger.info("--- Starting Player Birth Date Scraper ---")
        
        try:
            # Find players who have a bball-ref ID but are missing a birth date
            players_to_update = session.query(Player).filter(
                Player.player_id != None,
                Player.birth_date == None
            ).all()
            
            if not players_to_update:
                logger.info("No players found needing a birth date update. DB is up-to-date.")
                return

            logger.info(f"Found {len(players_to_update)} players to update.")
            
            updated_count = 0
            batch_size = 25 # Commit changes in batches
            batch_updated_count = 0

            for i, player in enumerate(players_to_update, 1):
                logger.info(f"Processing {i}/{len(players_to_update)}: {player.name} ({player.player_id})")
                
                # --- POLITENESS: Wait between requests ---
                time.sleep(3.0) 
                
                birth_date = self._get_bbr_birth_date(player.player_id)
                
                if birth_date:
                    player.birth_date = birth_date
                    player.updated_at = datetime.utcnow()
                    updated_count += 1
                    batch_updated_count += 1
                    logger.info(f"  [+] Found: {birth_date}")
                
                # Commit in batches to save progress
                if (batch_updated_count > 0 and batch_updated_count % batch_size == 0) or (i == len(players_to_update)):
                    logger.info(f"\n--- Committing batch of {batch_updated_count} updates to database... ---")
                    try:
                        session.commit()
                        logger.info("--- Batch committed successfully. ---")
                        batch_updated_count = 0 # Reset batch counter
                    except exc.SQLAlchemyError as e:
                        logger.error(f"[!!!] Database commit failed: {e}")
                        session.rollback()
                        logger.warning("--- Rolled back changes for this batch. ---")

            logger.info(f"\n--- Birthday scraping complete. {updated_count} players updated. ---")
            
        except Exception as e:
            logger.error(f"An error occurred during the birthday scraping process: {e}")
            session.rollback()

    # --- Private Helper Methods (Scraping Logic) ---

    def _get_active_players_list(self, year: int) -> List[Dict[str, str]]:
        """Get list of active players for a season"""
        url = f"{self.base_url}/leagues/NBA_{year}_per_game.html"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            table = soup.find('table', {'id': 'per_game_stats'})
            if not table:
                logger.error(f"Could not find stats table for {year}")
                return []
            
            players = []
            tbody = table.find('tbody')
            
            if not tbody:
                logger.error(f"Could not find table body for {year}")
                return []
                
            for row in tbody.find_all('tr', class_=lambda x: x != 'thead'):
                if row.find('th', {'class': 'thead'}):
                    continue
                
                try:
                    player_cell = row.find('td', {'data-stat': 'name_display'})
                    if player_cell and player_cell.find('a'):
                        player_name = player_cell.text.strip()
                        player_url = player_cell.find('a')['href']
                        player_id = player_url.split('/')[-1].replace('.html', '')
                        players.append({
                            'name': player_name,
                            'player_id': player_id,
                            'url': player_url
                        })
                except Exception as e:
                    logger.error(f"Error parsing player row: {e}")
                    continue
            
            # Remove duplicates (players traded mid-season appear multiple times)
            unique_players = {p['player_id']: p for p in players}
            logger.info(f"Found {len(unique_players)} unique players for {year} season")
            return list(unique_players.values())
            
        except Exception as e:
            logger.error(f"Error getting active players for {year}: {e}")
            return []

    def _scrape_player_game_log(self, player_id: str, player_name: str, year: int) -> List[Dict[str, Any]]:
        """Scrape game-by-game stats for a specific player"""
        url = f"{self.base_url}/players/{player_id[0]}/{player_id}/gamelog/{year}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the game log table
            table = soup.find('table', {'id': 'player_game_log_reg'})
            if not table:
                logger.warning(f"No game log table found for {player_name} ({year})")
                return []
            
            games_data = []
            tbody = table.find('tbody')
            if not tbody:
                logger.warning(f"No table body found for {player_name} ({year})")
                return []
            
            for row in tbody.find_all('tr'):
                # Skip header rows and non-game rows
                if row.find('th', {'class': 'thead'}) or 'thead' in row.get('class', []):
                    continue
                
                try:
                    date_str_cell = row.find('td', {'data-stat': 'date'})
                    if not date_str_cell or not date_str_cell.text.strip():
                        continue # Skip empty rows
                    
                    date_str = date_str_cell.text.strip()
                    game_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    team = row.find('td', {'data-stat': 'team_name_abbr'}).text.strip()
                    home_away = row.find('td', {'data-stat': 'game_location'}).text.strip()
                    opponent = row.find('td', {'data-stat': 'opp_name_abbr'}).text.strip()
                    game_result_raw = row.find('td', {'data-stat': 'game_result'}).text.strip()
                    
                    game_score = None
                    if game_result_raw:
                        parts = game_result_raw.split(' ')
                        if len(parts) > 1:
                            game_score = parts[1].strip('()')
                    
                    gs = row.find('td', {'data-stat': 'is_starter'})
                    game_started = 1 if gs and gs.text.strip() == '*' else 0
                    
                    mp = row.find('td', {'data-stat': 'mp'}).text.strip() if row.find('td', {'data-stat': 'mp'}) else None
                    
                    def get_int(stat_name):
                        cell = row.find('td', {'data-stat': stat_name})
                        if cell and cell.text.strip():
                            try:
                                return int(cell.text.strip())
                            except ValueError:
                                return None
                        return None
                    
                    def get_float(stat_name):
                        cell = row.find('td', {'data-stat': stat_name})
                        if cell and cell.text.strip():
                            try:
                                return float(cell.text.strip())
                            except ValueError:
                                return None
                        return None
                    
                    games_data.append({
                        'game_date': game_date, 'season': year, 'team': team,
                        'home_away': home_away, 'opponent': opponent,
                        'game_result': game_result_raw.split()[0] if game_result_raw else None,
                        'game_score': game_score, 'game_started': game_started,
                        'minutes_played': mp, 'field_goals': get_int('fg'),
                        'field_goal_attempts': get_int('fga'), 'field_goal_pct': get_float('fg_pct'),
                        'three_pointers': get_int('fg3'), 'three_point_attempts': get_int('fg3a'),
                        'three_point_pct': get_float('fg3_pct'), 'free_throws': get_int('ft'),
                        'free_throw_attempts': get_int('fta'), 'free_throw_pct': get_float('ft_pct'),
                        'offensive_rebounds': get_int('orb'), 'defensive_rebounds': get_int('drb'),
                        'total_rebounds': get_int('trb'), 'assists': get_int('ast'),
                        'steals': get_int('stl'), 'blocks': get_int('blk'),
                        'turnovers': get_int('tov'), 'personal_fouls': get_int('pf'),
                        'points': get_int('pts'), 'game_score_stat': get_float('game_score'),
                        'plus_minus': get_int('plus_minus')
                    })
                    
                except Exception as e:
                    logger.error(f"Error parsing game row for {player_name}: {e}")
                    continue
            
            return games_data
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Player game log page not found (404) for {player_name} at {url}")
            else:
                logger.error(f"HTTP Error scraping game log for {player_name}: {e}")
        except Exception as e:
            logger.error(f"General error scraping game log for {player_name}: {e}")
            
        return []

    def _get_bbr_birth_date(self, player_id: str) -> Union[datetime.date, None]:
        """
        Fetches a player's page and scrapes their birth date.
        """
        if not player_id:
            return None
        
        letter = player_id[0]
        url = f"{self.base_url}/players/{letter}/{player_id}.html"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status() 
            soup = BeautifulSoup(response.text, 'html.parser')
            
            birth_span = soup.find('span', id='necro-birth')
            
            if birth_span and 'data-birth' in birth_span.attrs:
                date_str = birth_span['data-birth']
                birth_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                return birth_date
            else:
                logger.warning(f"  [!] Could not find 'data-birth' info for {player_id}.")
                return None
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"  [!] Player page not found (404) for {player_id} at {url}")
            else:
                logger.error(f"  [!] HTTP Error for {player_id}: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"  [!] Network error for {player_id}: {e}")
        except Exception as e:
            logger.error(f"  [!] An unexpected error occurred for {player_id}: {e}")
            
        return None

    # --- Private Helper Methods (Database Logic) ---
    
    def _save_games_to_db(self, session: Session, player_info: Dict[str, str], games_data: List[Dict[str, Any]]) -> int:
        """
        Save player and game data to PostgreSQL database.
        Returns the count of *new* games added.
        """
        new_games_added = 0
        
        # 1. Find or Create Player
        player = session.query(Player).filter_by(player_id=player_info['player_id']).first()
        
        if not player:
            player = Player(
                name=player_info['name'],
                player_id=player_info['player_id']
            )
            session.add(player)
            session.commit() # Commit player separately to get player.id
            session.refresh(player)
        
        # 2. Add Game Stats
        for game_data in games_data:
            existing_game = session.query(GameStats).filter_by(
                player_id=player.id,
                game_date=game_data['game_date']
            ).first()
            
            if existing_game:
                # Update existing game (in case of stat corrections)
                for key, value in game_data.items():
                    setattr(existing_game, key, value)
            else:
                # Create new game entry
                game_stats = GameStats(
                    player_id=player.id,
                    **game_data
                )
                session.add(game_stats)
                new_games_added += 1
        
        # 3. Commit all games for this player
        if new_games_added > 0:
            session.commit()
        
        return new_games_added