# src/data_pipeline/collectors.py
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


class NCAADataCollector:
    """
    A data collector class for fetching NCAA football data from the SportsData.IO API.

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
            host=config.get('redis_host', 'localhost'),
            port=config.get('redis_port', 6379),
            decode_responses=True
        )

        self.base_endpoint = 'https://api.sportsdata.io/v3/cfb/scores/json'
        self.api_key = os.getenv("SPORTSDATAIO_API_KEY")

    async def _fetch_data(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        params: Dict = None
    ) -> Optional[List[Dict]]:
        """
        Private method to fetch data from APIs with caching and error handling.
        """
        if params is None:
            params = {}

        params['key'] = self.api_key
        url = f"{self.base_endpoint}/{endpoint}"

        cache_key = f"ncaa_cache:{url}?{'&'.join([f'{k}={v}' for k, v in sorted(params.items()) if k != 'key'])}"

        try:
            cached_data = self.redis_client.get(cache_key)
            if cached_data:
                self.logger.info(f"Cache hit for {cache_key}")
                return json.loads(cached_data)
        except Exception as e:
            self.logger.warning(f"Redis cache read error: {e}")

        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    json_data = await response.json()
                    try:
                        self.redis_client.setex(
                            cache_key,
                            3600,
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

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_teams_data(self) -> List[Dict]:
        """Collect data for all FBS teams."""
        endpoint = "Teams"
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint)
            if data:
                self.logger.info(f"Successfully collected data for {len(data)} teams")
                return data
            else:
                self.logger.error("Failed to collect teams data")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_schedule(self, season: int) -> List[Dict]:
        """Collect game schedule data for a given season."""
        endpoint = f"Games/{season}"
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint)
            if data:
                self.logger.info(f"Successfully collected {len(data)} games for {season} season")
                return data
            else:
                self.logger.error(f"Failed to collect schedule data for {season}")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_game_stats_by_week(self, season: int, week: int) -> List[Dict]:
        """Collect game statistics for a specific week."""
        endpoint = f"GameStatsByWeek/{season}/{week}"
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint)
            if data:
                self.logger.info(f"Successfully collected game stats for {len(data)} games")
                return data
            else:
                self.logger.error(f"Failed to collect game stats for season {season}, week {week}")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_player_season_stats(self, season: int, category: str = None) -> List[Dict]:
        """Collect player statistics for a season."""
        endpoint = f"PlayerSeasonStats/{season}"
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint)
            if data:
                self.logger.info(f"Successfully collected player stats for {len(data)} players")
                return data
            else:
                self.logger.error(f"Failed to collect player stats for {season}")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_team_season_stats(self, season: int) -> List[Dict]:
        """Collect team statistics for a season."""
        endpoint = f"TeamSeasonStats/{season}"
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint)
            if data:
                self.logger.info(f"Successfully collected team stats for {len(data)} teams")
                return data
            else:
                self.logger.error(f"Failed to collect team season stats for {season}")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_team_advanced_season_stats(self, season: int) -> List[Dict]:
        """
        Collect advanced team statistics (efficiency, explosiveness, etc.).
        This method now fetches data from the TeamSeasonStats endpoint, which
        is comprehensive.
        """
        return await self.collect_team_season_stats(season)

    @sleep_and_retry
    @limits(calls=450, period=3600)
    async def collect_betting_lines(self, game_id: int) -> List[Dict]:
        """Collect betting odds for a specific game."""
        endpoint = f"GameOddsByGameID/{game_id}"
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint)
            if data:
                self.logger.info(f"Successfully collected {len(data)} betting lines for game {game_id}")
                return data
            else:
                self.logger.error(f"Failed to collect betting lines for game {game_id}")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_player_game_stats(self, season: int, week: int) -> List[Dict]:
        """Collect individual player game statistics."""
        endpoint = f"PlayerGameStatsByWeek/{season}/{week}"
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint)
            if data:
                self.logger.info(f"Successfully collected player game stats for {len(data)} players")
                return data
            else:
                self.logger.error(f"Failed to collect player game stats for season {season}, week {week}")
                return []

    @sleep_and_retry
    @limits(calls=50, period=60)
    async def collect_team_game_stats(self, season: int, week: int) -> List[Dict]:
        """Collect team game statistics (detailed box scores)."""
        endpoint = f"TeamGameStatsByWeek/{season}/{week}"
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_data(session, endpoint)
            if data:
                self.logger.info(f"Successfully collected team game stats for {len(data)} games")
                return data
            else:
                self.logger.error(f"Failed to collect team game stats for season {season}, week {week}")
                return []