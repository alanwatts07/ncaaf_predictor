import asyncio
import aiohttp
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
from ratelimit import limits, sleep_and_retry
import redis


class NCAADataCollector:
    """
    A data collector class for fetching NCAA football data from various sports APIs.
    
    This class handles fetching team data, schedules, game statistics, and betting lines
    with built-in caching, rate limiting, and error handling.
    """
    
    def __init__(self, config: Dict) -> None:
        """
        Initialize the NCAA Data Collector.
        
        Args:
            config (Dict): Configuration dictionary containing API keys, Redis settings, etc.
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.redis_client = redis.Redis(
            host=config['redis_host'], 
            port=config.get('redis_port', 6379),
            decode_responses=True
        )
        
        self.base_endpoints = {
            'cfbd': 'https://api.collegefootballdata.com',
            'odds': 'https://api.the-odds-api.com/v4'
        }
    
    async def _fetch_data(
        self, 
        session: aiohttp.ClientSession, 
        url: str, 
        headers: Dict = None, 
        params: Dict = None
    ) -> Optional[Dict]:
        """
        Private method to fetch data from APIs with caching and error handling.
        
        Args:
            session (aiohttp.ClientSession): The aiohttp session to use for requests
            url (str): The URL to fetch data from
            headers (Dict, optional): HTTP headers to include in the request
            params (Dict, optional): Query parameters to include in the request
            
        Returns:
            Optional[Dict]: The JSON response data or None if request failed
        """
        # Create cache key from URL and params
        cache_key = f"ncaa_cache:{url}"
        if params:
            param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
            cache_key += f"?{param_str}"
        
        # Check Redis cache first
        try:
            cached_data = self.redis_client.get(cache_key)
            if cached_data:
                self.logger.info(f"Cache hit for {cache_key}")
                return json.loads(cached_data)
        except Exception as e:
            self.logger.warning(f"Redis cache read error: {e}")
        
        # Make web request if not in cache
        try:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    json_data = await response.json()
                    
                    # Cache the successful response
                    try:
                        self.redis_client.setex(
                            cache_key, 
                            3600,  # 1 hour expiration
                            json.dumps(json_data, default=str)
                        )
                        self.logger.info(f"Cached response for {cache_key}")
                    except Exception as e:
                        self.logger.warning(f"Redis cache write error: {e}")
                    
                    return json_data
                else:
                    self.logger.error(f"HTTP {response.status} error for URL: {url}")
                    return None
                    
        except aiohttp.ClientError as e:
            self.logger.error(f"Connection error for URL {url}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error for URL {url}: {e}")
            return None
    # Add these methods to your NCAADataCollector class:

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_player_season_stats(self, season: int, category: str = None) -> List[Dict]:
        """
        Collect detailed player statistics for a season.
        
        Args:
            season (int): The season year
            category (str): Stats category ('passing', 'rushing', 'receiving', 'defensive', etc.)
        
        Returns:
            List[Dict]: List of player statistics
        """
        url = f"{self.base_endpoints['cfbd']}/stats/player/season"
        headers = {
            'Authorization': f"Bearer {self.config.get('cfbd_api_key', '')}"
        }
        params = {'year': season}
        if category:
            params['category'] = category
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, url, headers=headers, params=params)
            
            if data:
                self.logger.info(f"Successfully collected {len(data)} player stats for {season}")
                return data
            else:
                self.logger.error(f"Failed to collect player stats for {season}")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_team_season_stats(self, season: int) -> List[Dict]:
        """
        Collect comprehensive team statistics for a season.
        
        Args:
            season (int): The season year
        
        Returns:
            List[Dict]: List of team season statistics
        """
        url = f"{self.base_endpoints['cfbd']}/stats/season"
        headers = {
            'Authorization': f"Bearer {self.config.get('cfbd_api_key', '')}"
        }
        params = {'year': season}
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, url, headers=headers, params=params)
            
            if data:
                self.logger.info(f"Successfully collected team season stats for {len(data)} teams in {season}")
                return data
            else:
                self.logger.error(f"Failed to collect team season stats for {season}")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_team_advanced_season_stats(self, season: int) -> List[Dict]:
        """
        Collect advanced team statistics (efficiency, explosiveness, etc.).
        
        Args:
            season (int): The season year
        
        Returns:
            List[Dict]: List of advanced team statistics
        """
        url = f"{self.base_endpoints['cfbd']}/stats/season/advanced"
        headers = {
            'Authorization': f"Bearer {self.config.get('cfbd_api_key', '')}"
        }
        params = {'year': season}
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, url, headers=headers, params=params)
            
            if data:
                self.logger.info(f"Successfully collected advanced team stats for {len(data)} teams in {season}")
                return data
            else:
                self.logger.error(f"Failed to collect advanced team stats for {season}")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_team_talent_ratings(self, season: int) -> List[Dict]:
        """
        Collect team talent composite ratings.
        
        Args:
            season (int): The season year
        
        Returns:
            List[Dict]: List of team talent ratings
        """
        url = f"{self.base_endpoints['cfbd']}/talent"
        headers = {
            'Authorization': f"Bearer {self.config.get('cfbd_api_key', '')}"
        }
        params = {'year': season}
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, url, headers=headers, params=params)
            
            if data:
                self.logger.info(f"Successfully collected talent ratings for {len(data)} teams in {season}")
                return data
            else:
                self.logger.error(f"Failed to collect talent ratings for {season}")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_recruiting_data(self, season: int) -> List[Dict]:
        """
        Collect recruiting class data.
        
        Args:
            season (int): The recruiting class year
        
        Returns:
            List[Dict]: List of recruiting data
        """
        url = f"{self.base_endpoints['cfbd']}/recruiting/teams"
        headers = {
            'Authorization': f"Bearer {self.config.get('cfbd_api_key', '')}"
        }
        params = {'year': season}
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, url, headers=headers, params=params)
            
            if data:
                self.logger.info(f"Successfully collected recruiting data for {len(data)} teams in {season}")
                return data
            else:
                self.logger.error(f"Failed to collect recruiting data for {season}")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_player_game_stats(self, season: int, week: int = None, team: str = None) -> List[Dict]:
        """
        Collect individual player game statistics.
        
        Args:
            season (int): The season year
            week (int, optional): Specific week
            team (str, optional): Specific team
        
        Returns:
            List[Dict]: List of player game statistics
        """
        url = f"{self.base_endpoints['cfbd']}/games/players"
        headers = {
            'Authorization': f"Bearer {self.config.get('cfbd_api_key', '')}"
        }
        params = {'year': season}
        if week:
            params['week'] = week
        if team:
            params['team'] = team
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, url, headers=headers, params=params)
            
            if data:
                self.logger.info(f"Successfully collected player game stats: {len(data)} games")
                return data
            else:
                self.logger.error(f"Failed to collect player game stats")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_team_game_stats(self, season: int, week: int = None) -> List[Dict]:
        """
        Collect team game statistics (detailed box scores).
        
        Args:
            season (int): The season year
            week (int, optional): Specific week
        
        Returns:
            List[Dict]: List of team game statistics
        """
        url = f"{self.base_endpoints['cfbd']}/games/teams"
        headers = {
            'Authorization': f"Bearer {self.config.get('cfbd_api_key', '')}"
        }
        params = {'year': season}
        if week:
            params['week'] = week
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, url, headers=headers, params=params)
            
            if data:
                self.logger.info(f"Successfully collected team game stats: {len(data)} games")
                return data
            else:
                self.logger.error(f"Failed to collect team game stats")
                return []
                
    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_teams_data(self) -> List[Dict]:
        """
        Collect data for all FBS teams.
        
        Returns:
            List[Dict]: List of team dictionaries containing school, conference, mascot, etc.
        """
        url = f"{self.base_endpoints['cfbd']}/teams/fbs"
        headers = {
            'Authorization': f"Bearer {self.config.get('cfbd_api_key', '')}"
        }
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, url, headers=headers)
            
            if data:
                # Process and structure team data
                teams = []
                for team in data:
                    team_dict = {
                        'school': team.get('school'),
                        'conference': team.get('conference'),
                        'division': team.get('division'),
                        'mascot': team.get('mascot'),
                        'abbreviation': team.get('abbreviation'),
                        'alt_name_1': team.get('alt_name_1'),
                        'alt_name_2': team.get('alt_name_2'),
                        'alt_name_3': team.get('alt_name_3'),
                        'color': team.get('color'),
                        'alt_color': team.get('alt_color'),
                        'logos': team.get('logos', [])
                    }
                    teams.append(team_dict)
                
                self.logger.info(f"Successfully collected data for {len(teams)} teams")
                return teams
            else:
                self.logger.error("Failed to collect teams data")
                return []
    
    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_schedule(self, season: int) -> List[Dict]:
        """
        Collect game schedule data for a given season.
        
        Args:
            season (int): The season year to collect schedule for
            
        Returns:
            List[Dict]: List of game dictionaries with schedule information
        """
        url = f"{self.base_endpoints['cfbd']}/games"
        headers = {
            'Authorization': f"Bearer {self.config.get('cfbd_api_key', '')}"
        }
        params = {
            'year': season,
            'division': 'fbs'
        }
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, url, headers=headers, params=params)
            
            if data:
                games = []
                for game in data:
                    # Parse start_date string to datetime object
                    start_date = None
                    if game.get('start_date'):
                        try:
                            start_date = datetime.fromisoformat(
                                game['start_date'].replace('Z', '+00:00')
                            )
                        except (ValueError, AttributeError) as e:
                            self.logger.warning(f"Could not parse date {game.get('start_date')}: {e}")
                    
                    game_dict = {
                        'id': game.get('id'),
                        'season': game.get('season'),
                        'week': game.get('week'),
                        'season_type': game.get('season_type'),
                        'start_date': start_date,
                        'home_team': game.get('home_team'),
                        'away_team': game.get('away_team'),
                        'home_points': game.get('home_points'),
                        'away_points': game.get('away_points'),
                        'venue': game.get('venue'),
                        'venue_id': game.get('venue_id'),
                        'neutral_site': game.get('neutral_site'),
                        'conference_game': game.get('conference_game'),
                        'attendance': game.get('attendance')
                    }
                    games.append(game_dict)
                
                self.logger.info(f"Successfully collected {len(games)} games for {season} season")
                return games
            else:
                self.logger.error(f"Failed to collect schedule data for {season}")
                return []
    
    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_game_stats(self, game_id: int) -> Optional[Dict]:
        """
        Collect advanced team statistics for a specific game.
        
        Args:
            game_id (int): The unique identifier for the game
            
        Returns:
            Optional[Dict]: Dictionary with home and away team stats, or None if failed
        """
        url = f"{self.base_endpoints['cfbd']}/stats/game/advanced"
        headers = {
            'Authorization': f"Bearer {self.config.get('cfbd_api_key', '')}"
        }
        params = {'gameId': game_id}
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, url, headers=headers, params=params)
            
            if data and isinstance(data, list) and len(data) >= 2:
                # Process the two team stats into a structured format
                game_stats = {}
                
                for team_stats in data:
                    team_name = team_stats.get('team')
                    if not team_name:
                        continue
                    
                    # Extract key statistics
                    stats = team_stats.get('stats', {})
                    processed_stats = {
                        'team': team_name,
                        'totalYards': stats.get('totalYards'),
                        'netPassingYards': stats.get('netPassingYards'),
                        'rushingYards': stats.get('rushingYards'),
                        'turnovers': stats.get('turnovers'),
                        'thirdDownEff': stats.get('thirdDownEff'),
                        'sacks': stats.get('sacks'),
                        'tacklesForLoss': stats.get('tacklesForLoss'),
                        'passCompletionPercentage': stats.get('passCompletionPercentage'),
                        'timeOfPossession': stats.get('timeOfPossession'),
                        'firstDowns': stats.get('firstDowns'),
                        'fourthDownEff': stats.get('fourthDownEff'),
                        'penalties': stats.get('penalties'),
                        'penaltyYards': stats.get('penaltyYards')
                    }
                    
                    # Determine if this is home or away team (simplified approach)
                    if not game_stats:
                        game_stats['team_1'] = processed_stats
                    else:
                        game_stats['team_2'] = processed_stats
                
                self.logger.info(f"Successfully collected game stats for game {game_id}")
                return game_stats
            else:
                self.logger.error(f"Failed to collect game stats for game {game_id}")
                return None
    
    @sleep_and_retry
    @limits(calls=450, period=3600)
    async def collect_betting_lines(self, season: int) -> List[Dict]:
        """
        Collect betting odds and lines for NCAA football games.
        
        Args:
            season (int): The season year to collect betting data for
            
        Returns:
            List[Dict]: List of betting line dictionaries from various bookmakers
        """
        url = f"{self.base_endpoints['odds']}/sports/americanfootball_ncaaf/odds"
        params = {
            'apiKey': self.config.get('odds_api_key', ''),
            'regions': 'us',
            'markets': 'spreads,totals',
            'oddsFormat': 'american',
            'dateFormat': 'iso'
        }
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, url, params=params)
            
            if data and isinstance(data, list):
                betting_lines = []
                
                for game in data:
                    home_team = game.get('home_team')
                    away_team = game.get('away_team')
                    commence_time = game.get('commence_time')
                    
                    # Parse commence time
                    game_time = None
                    if commence_time:
                        try:
                            game_time = datetime.fromisoformat(
                                commence_time.replace('Z', '+00:00')
                            )
                        except (ValueError, AttributeError) as e:
                            self.logger.warning(f"Could not parse game time {commence_time}: {e}")
                    
                    # Process each bookmaker
                    for bookmaker in game.get('bookmakers', []):
                        bookmaker_name = bookmaker.get('key')
                        
                        line_data = {
                            'home_team': home_team,
                            'away_team': away_team,
                            'commence_time': game_time,
                            'bookmaker': bookmaker_name,
                            'spread_point': None,
                            'spread_home_price': None,
                            'spread_away_price': None,
                            'total_point': None,
                            'total_over_price': None,
                            'total_under_price': None
                        }
                        
                        # Extract spreads and totals from markets
                        for market in bookmaker.get('markets', []):
                            market_key = market.get('key')
                            
                            if market_key == 'spreads':
                                outcomes = market.get('outcomes', [])
                                for outcome in outcomes:
                                    if outcome.get('name') == home_team:
                                        line_data['spread_point'] = outcome.get('point')
                                        line_data['spread_home_price'] = outcome.get('price')
                                    elif outcome.get('name') == away_team:
                                        line_data['spread_away_price'] = outcome.get('price')
                            
                            elif market_key == 'totals':
                                outcomes = market.get('outcomes', [])
                                for outcome in outcomes:
                                    if outcome.get('name') == 'Over':
                                        line_data['total_point'] = outcome.get('point')
                                        line_data['total_over_price'] = outcome.get('price')
                                    elif outcome.get('name') == 'Under':
                                        line_data['total_under_price'] = outcome.get('price')
                        
                        betting_lines.append(line_data)
                
                self.logger.info(f"Successfully collected {len(betting_lines)} betting lines")
                return betting_lines
            else:
                self.logger.error("Failed to collect betting lines data")
                return []