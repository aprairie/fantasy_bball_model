import requests
from bs4 import BeautifulSoup
import time
import logging
from datetime import datetime
from database import SessionLocal, Player, GameStats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BasketballScraper:
    def __init__(self):
        self.base_url = "https://www.basketball-reference.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def get_active_players(self, year):
        """Get list of active players for a season"""
        url = f"{self.base_url}/leagues/NBA_{year}_per_game.html"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            table = soup.find('table', {'id': 'per_game_stats'})
            if not table:
                logger.error("Could not find stats table")
                return []
            
            players = []
            tbody = table.find('tbody')
            
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
            logger.error(f"Error getting active players: {e}")
            return []
    
    def scrape_player_game_log(self, player_id, player_name, year):
        """Scrape game-by-game stats for a specific player"""
        url = f"{self.base_url}/players/{player_id[0]}/{player_id}/gamelog/{year}"
        
        try:
            logger.info(f"Scraping game log for {player_name} ({year})")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the game log table
            table = soup.find('table', {'id': 'player_game_log_reg'})
            if not table:
                logger.warning(f"No game log table found for {player_name}")
                return []
            
            games_data = []
            tbody = table.find('tbody')
            
            for row in tbody.find_all('tr'):
                # Skip header rows and non-game rows
                if row.find('th', {'class': 'thead'}) or 'thead' in row.get('class', []):
                    continue
                
                try:
                    date_str = row.find('td', {'data-stat': 'date'})
                    if not date_str:
                        continue
                    
                    game_date = datetime.strptime(date_str.text.strip(), '%Y-%m-%d').date()
                    team = row.find('td', {'data-stat': 'team_name_abbr'}).text.strip()
                    home_away = row.find('td', {'data-stat': 'game_location'}).text.strip()
                    opponent = row.find('td', {'data-stat': 'opp_name_abbr'}).text.strip()
                    game_result = row.find('td', {'data-stat': 'game_result'}).text.strip()
                    
                    # Parse game result for score
                    game_score = None
                    if game_result:
                        parts = game_result.split(' ')
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
                        'game_date': game_date,
                        'season': year,
                        'team': team,
                        'home_away': home_away,
                        'opponent': opponent,
                        'game_result': game_result.split()[0] if game_result else None,
                        'game_score': game_score,
                        'game_started': game_started,
                        'minutes_played': mp,
                        'field_goals': get_int('fg'),
                        'field_goal_attempts': get_int('fga'),
                        'field_goal_pct': get_float('fg_pct'),
                        'three_pointers': get_int('fg3'),
                        'three_point_attempts': get_int('fg3a'),
                        'three_point_pct': get_float('fg3_pct'),
                        'free_throws': get_int('ft'),
                        'free_throw_attempts': get_int('fta'),
                        'free_throw_pct': get_float('ft_pct'),
                        'offensive_rebounds': get_int('orb'),
                        'defensive_rebounds': get_int('drb'),
                        'total_rebounds': get_int('trb'),
                        'assists': get_int('ast'),
                        'steals': get_int('stl'),
                        'blocks': get_int('blk'),
                        'turnovers': get_int('tov'),
                        'personal_fouls': get_int('pf'),
                        'points': get_int('pts'),
                        'game_score_stat': get_float('game_score'),
                        'plus_minus': get_int('plus_minus')
                    })
                    
                except Exception as e:
                    logger.error(f"Error parsing game row for {player_name}: {e}")
                    continue
            
            logger.info(f"Scraped {len(games_data)} games for {player_name}")
            return games_data
            
        except Exception as e:
            logger.error(f"Error scraping game log for {player_name}: {e}")
            return []
    
    def save_to_database(self, player_info, games_data):
        """Save player and game data to PostgreSQL database"""
        db = SessionLocal()
        try:
            # Check if player exists
            player = db.query(Player).filter_by(player_id=player_info['player_id']).first()
            
            if not player:
                player = Player(
                    name=player_info['name'],
                    player_id=player_info['player_id']
                )
                db.add(player)
                db.commit()
                db.refresh(player)
                logger.info(f"Added new player: {player_info['name']}")
            
            # Add game stats
            for game_data in games_data:
                # Check if this game already exists
                existing_game = db.query(GameStats).filter_by(
                    player_id=player.id,
                    game_date=game_data['game_date']
                ).first()
                
                if existing_game:
                    # Update existing game
                    for key, value in game_data.items():
                        setattr(existing_game, key, value)
                else:
                    # Create new game entry
                    game_stats = GameStats(
                        player_id=player.id,
                        **game_data
                    )
                    db.add(game_stats)
            
            db.commit()
            logger.info(f"Saved {len(games_data)} games for {player_info['name']}")
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error saving to database: {e}")
        finally:
            db.close()

def main():
    scraper = BasketballScraper()
    years = [2026, 2025, 2024, 2023]
    
    for year in years:
        logger.info(f"Starting scrape for {year} season...")
        
        # Get list of active players
        players = scraper.get_active_players(year)
        
        if not players:
            logger.error("No players found")
            return
        
        logger.info(f"Will scrape game logs for {len(players)} players")
        
        # Scrape game logs for each player
        for i, player_info in enumerate(players, 1):
            logger.info(f"Processing player {i}/{len(players)}: {player_info['name']}")
            
            games_data = scraper.scrape_player_game_log(
                player_info['player_id'],
                player_info['name'],
                year
            )
            
            if games_data:
                scraper.save_to_database(player_info, games_data)
            
            # Be respectful to the server - add delay between requests
            if i < len(players):
                time.sleep(3)
    
    logger.info("Scraping completed successfully!")

if __name__ == "__main__":
    main()