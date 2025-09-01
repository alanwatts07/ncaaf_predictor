# src/data_pipeline/cfbd_collector.py
import asyncio
import aiohttp
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
from ratelimit import limits, sleep_and_retry
import redis
import os
from dotenv import load_dotenv

load_dotenv()


class CFBDAdvancedCollector:
    """
    Advanced data collector for CFBD API focusing on opponent-adjusted metrics.
    
    This collector targets the most predictive metrics including SP+ ratings,
    opponent-adjusted efficiency stats, and advanced team analytics.
    """

    def __init__(self, config: Dict) -> None:
        """
        Initialize the CFBD Advanced Collector.

        Args:
            config (Dict): Configuration dictionary containing API keys, Redis settings, etc.
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.redis_client = redis.Redis(
            host=config.get('redis_host', 'localhost'),
            port=config.get('redis_port', 6379),
            decode_responses=True
        )

        self.base_endpoint = 'https://api.collegefootballdata.com'
        self.api_key = config.get('cfbd_api_key') or os.getenv("CFBD_API_KEY")
        
        if not self.api_key:
            raise ValueError("CFBD API key is required. Set CFBD_API_KEY environment variable.")

    async def _fetch_data(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        params: Dict = None
    ) -> Optional[List[Dict]]:
        """
        Private method to fetch data from CFBD API with caching and error handling.
        """
        if params is None:
            params = {}

        url = f"{self.base_endpoint}/{endpoint}"
        
        # Create cache key
        cache_key = f"cfbd_cache:{endpoint}:{'&'.join([f'{k}={v}' for k, v in sorted(params.items())])}"

        # Try cache first
        try:
            cached_data = self.redis_client.get(cache_key)
            if cached_data:
                self.logger.info(f"Cache hit for {endpoint}")
                return json.loads(cached_data)
        except Exception as e:
            self.logger.warning(f"Redis cache read error: {e}")

        # Set headers for CFBD API
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Accept': 'application/json'
        }

        try:
            async with session.get(url, params=params, headers=headers) as response:
                if response.status == 200:
                    json_data = await response.json()
                    try:
                        # Cache for 1 hour for most data, 6 hours for historical
                        cache_time = 21600 if params.get('year', 2024) < 2024 else 3600
                        self.redis_client.setex(
                            cache_key,
                            cache_time,
                            json.dumps(json_data, default=str)
                        )
                        self.logger.info(f"Cached response for {endpoint}")
                    except Exception as e:
                        self.logger.warning(f"Redis cache write error: {e}")
                    return json_data
                else:
                    self.logger.error(f"HTTP {response.status} error for {endpoint}: {await response.text()}")
                    return None
        except aiohttp.ClientError as e:
            self.logger.error(f"Connection error for {endpoint}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error for {endpoint}: {e}")
            return None

    @sleep_and_retry
    @limits(calls=100, period=60)  # CFBD rate limit
    async def collect_sp_plus_ratings(self, year: int = None) -> List[Dict]:
        """
        Collect SP+ ratings - the gold standard for opponent-adjusted team strength.
        
        SP+ ratings are Bill Connelly's opponent-adjusted efficiency ratings that
        separate offense, defense, and special teams while adjusting for opponent strength.
        """
        endpoint = "ratings/sp"
        params = {}
        if year:
            params['year'] = year
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint, params)
            if data:
                self.logger.info(f"Successfully collected SP+ ratings for {len(data)} teams")
                return data
            else:
                self.logger.error("Failed to collect SP+ ratings")
                return []

    @sleep_and_retry
    @limits(calls=100, period=60)
    async def collect_advanced_team_stats(self, year: int, start_week: int = None, end_week: int = None) -> List[Dict]:
        """
        Collect advanced team statistics including efficiency metrics.
        
        These include opponent-adjusted offensive and defensive efficiency ratings
        that are crucial for prediction models.
        """
        endpoint = "stats/season/advanced"
        params = {'year': year}
        if start_week:
            params['startWeek'] = start_week
        if end_week:
            params['endWeek'] = end_week
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint, params)
            if data:
                self.logger.info(f"Successfully collected advanced team stats for {len(data)} teams")
                return data
            else:
                self.logger.error(f"Failed to collect advanced team stats for {year}")
                return []

    @sleep_and_retry
    @limits(calls=100, period=60)
    async def collect_ppa_team_stats(self, year: int, team: str = None, conference: str = None) -> List[Dict]:
        """
        Collect team Predicted Points Added (PPA) statistics.
        
        PPA is a key component of advanced metrics, measuring the expected points
        added on each play adjusted for situation and field position.
        """
        endpoint = "ppa/teams"
        params = {'year': year}
        if team:
            params['team'] = team
        if conference:
            params['conference'] = conference
            
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint, params)
            if data:
                self.logger.info(f"Successfully collected PPA team stats for {len(data)} teams")
                return data
            else:
                self.logger.error(f"Failed to collect PPA team stats for {year}")
                return []

    @sleep_and_retry
    @limits(calls=100, period=60)
    async def collect_fpi_ratings(self, year: int = None, team: str = None, conference: str = None) -> List[Dict]:
        """
        Collect Football Power Index (FPI) ratings.
        
        FPI is ESPN's opponent-adjusted efficiency rating system.
        """
        endpoint = "ratings/fpi"
        params = {}
        if year:
            params['year'] = year
        if team:
            params['team'] = team
        if conference:
            params['conference'] = conference
            
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint, params)
            if data:
                self.logger.info(f"Successfully collected FPI ratings for {len(data)} teams")
                return data
            else:
                self.logger.error("Failed to collect FPI ratings")
                return []

    @sleep_and_retry
    @limits(calls=100, period=60)
    async def collect_opponent_adjusted_stats(self, year: int, team: str = None, conference: str = None) -> List[Dict]:
        """
        Collect opponent-adjusted team statistics.
        
        These are the key metrics that adjust raw stats for strength of schedule
        and opponent quality - critical for accurate predictions.
        """
        endpoint = "stats/season/advanced"
        params = {'year': year}
        if team:
            params['team'] = team
        if conference:
            params['conference'] = conference
            
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint, params)
            if data:
                # Filter for opponent-adjusted metrics specifically
                adjusted_data = []
                for team_data in data:
                    # Extract opponent-adjusted metrics
                    adjusted_metrics = {
                        'team': team_data.get('team'),
                        'conference': team_data.get('conference'),
                        'year': year,
                        
                        # Opponent-adjusted offensive metrics
                        'off_total_ppa': team_data.get('offense', {}).get('totalPPA'),
                        'off_success_rate': team_data.get('offense', {}).get('successRate'),
                        'off_explosiveness': team_data.get('offense', {}).get('explosiveness'),
                        'off_rushing_ppa': team_data.get('offense', {}).get('rushingPPA'),
                        'off_passing_ppa': team_data.get('offense', {}).get('passingPPA'),
                        'off_standard_downs_ppa': team_data.get('offense', {}).get('standardDownsPPA'),
                        'off_passing_downs_ppa': team_data.get('offense', {}).get('passingDownsPPA'),
                        
                        # Opponent-adjusted defensive metrics  
                        'def_total_ppa': team_data.get('defense', {}).get('totalPPA'),
                        'def_success_rate': team_data.get('defense', {}).get('successRate'),
                        'def_explosiveness': team_data.get('defense', {}).get('explosiveness'),
                        'def_rushing_ppa': team_data.get('defense', {}).get('rushingPPA'),
                        'def_passing_ppa': team_data.get('defense', {}).get('passingPPA'),
                        'def_standard_downs_ppa': team_data.get('defense', {}).get('standardDownsPPA'),
                        'def_passing_downs_ppa': team_data.get('defense', {}).get('passingDownsPPA'),
                    }
                    adjusted_data.append(adjusted_metrics)
                
                self.logger.info(f"Successfully processed opponent-adjusted stats for {len(adjusted_data)} teams")
                return adjusted_data
            else:
                self.logger.error(f"Failed to collect opponent-adjusted stats for {year}")
                return []

    @sleep_and_retry
    @limits(calls=100, period=60)
    async def collect_recruiting_rankings(self, year: int, team: str = None, conference: str = None) -> List[Dict]:
        """
        Collect recruiting rankings - important for long-term team strength assessment.
        """
        endpoint = "recruiting/teams"
        params = {'year': year}
        if team:
            params['team'] = team
        if conference:
            params['conference'] = conference
            
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint, params)
            if data:
                self.logger.info(f"Successfully collected recruiting rankings for {len(data)} teams")
                return data
            else:
                self.logger.error(f"Failed to collect recruiting rankings for {year}")
                return []

    @sleep_and_retry
    @limits(calls=100, period=60)
    async def collect_team_talent_rankings(self, year: int) -> List[Dict]:
        """
        Collect team talent composite rankings - measures overall roster talent.
        """
        endpoint = "talent"
        params = {'year': year}
        
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint, params)
            if data:
                self.logger.info(f"Successfully collected talent rankings for {len(data)} teams")
                return data
            else:
                self.logger.error(f"Failed to collect talent rankings for {year}")
                return []

    @sleep_and_retry
    @limits(calls=100, period=60)
    async def collect_game_ppa_stats(self, year: int, week: int = None, team: str = None, conference: str = None) -> List[Dict]:
        """
        Collect game-level PPA statistics for more granular analysis.
        """
        endpoint = "ppa/games"
        params = {'year': year}
        if week:
            params['week'] = week
        if team:
            params['team'] = team
        if conference:
            params['conference'] = conference
            
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint, params)
            if data:
                self.logger.info(f"Successfully collected game PPA stats for {len(data)} games")
                return data
            else:
                self.logger.error(f"Failed to collect game PPA stats for {year}")
                return []

    async def collect_comprehensive_advanced_data(self, year: int) -> Dict[str, List[Dict]]:
        """
        Collect all the key opponent-adjusted and advanced metrics for a season.
        
        This is the main method that gathers all the most predictive data points.
        """
        self.logger.info(f"Starting comprehensive advanced data collection for {year}")
        
        collected_data = {}
        
        try:
            # 1. SP+ Ratings (most important)
            self.logger.info("Collecting SP+ ratings...")
            sp_plus = await self.collect_sp_plus_ratings(year)
            collected_data['sp_plus_ratings'] = sp_plus
            await asyncio.sleep(1)
            
            # 2. Opponent-adjusted team statistics 
            self.logger.info("Collecting opponent-adjusted team statistics...")
            opponent_adj = await self.collect_opponent_adjusted_stats(year)
            collected_data['opponent_adjusted_stats'] = opponent_adj
            await asyncio.sleep(1)
            
            # 3. Advanced team statistics
            self.logger.info("Collecting advanced team statistics...")
            advanced_stats = await self.collect_advanced_team_stats(year)
            collected_data['advanced_team_stats'] = advanced_stats
            await asyncio.sleep(1)
            
            # 4. PPA team statistics
            self.logger.info("Collecting PPA team statistics...")
            ppa_stats = await self.collect_ppa_team_stats(year)
            collected_data['ppa_team_stats'] = ppa_stats
            await asyncio.sleep(1)
            
            # 5. FPI ratings
            self.logger.info("Collecting FPI ratings...")
            fpi_ratings = await self.collect_fpi_ratings(year)
            collected_data['fpi_ratings'] = fpi_ratings
            await asyncio.sleep(1)
            
            # 6. Recruiting rankings (for context)
            self.logger.info("Collecting recruiting rankings...")
            recruiting = await self.collect_recruiting_rankings(year)
            collected_data['recruiting_rankings'] = recruiting
            await asyncio.sleep(1)
            
            # 7. Talent rankings (for context)
            self.logger.info("Collecting talent rankings...")
            talent = await self.collect_team_talent_rankings(year)
            collected_data['talent_rankings'] = talent
            await asyncio.sleep(1)
            
            self.logger.info(f"Comprehensive advanced data collection completed for {year}")
            return collected_data
            
        except Exception as e:
            self.logger.error(f"Error in comprehensive collection for {year}: {e}")
            return collected_data

    def save_data(self, data: any, filename: str, output_dir: str = "data/raw/") -> None:
        """Save collected data to JSON file."""
        if not data:
            self.logger.warning(f"No data to save for {filename}")
            return
            
        output_path = f"{output_dir}/{filename}.json"
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            self.logger.info(f"Saved {filename}.json with {len(data) if isinstance(data, list) else len(data.keys())} records")
        except Exception as e:
            self.logger.error(f"Error saving {filename}: {e}")