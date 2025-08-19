# main.py
import asyncio
import json
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

# Import your collector and config
from src.data_pipeline.collectors import NCAADataCollector
from config import get_config, API_SETUP_INSTRUCTIONS

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collection.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class DataIngestionPipeline:
    """Main pipeline for ingesting NCAA football data."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.collector = NCAADataCollector(config)
        self.output_dir = Path(config['output_dir'])
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def save_data(self, data: Any, filename: str) -> None:
        """Save data to file in specified format."""
        if not data:
            logger.warning(f"No data to save for {filename}")
            return
        
        file_path = self.output_dir / filename
        
        if self.config['file_format'] == 'json':
            with open(f"{file_path}.json", 'w') as f:
                json.dump(data, f, indent=2, default=str)
        elif self.config['file_format'] == 'csv':
            import pandas as pd
            if isinstance(data, list):
                df = pd.DataFrame(data)
                df.to_csv(f"{file_path}.csv", index=False)
            else:
                logger.error(f"Cannot save non-list data as CSV: {filename}")
        
        logger.info(f"Saved {filename} with {len(data) if isinstance(data, list) else 1} records")
    
    async def collect_all_data(self, season: int) -> Dict[str, Any]:
        """Collect all data for a given season."""
        logger.info(f"Starting data collection for {season} season...")
        
        collected_data = {}
        
        try:
            # 1. Collect teams data (only need to do this once, not per season)
            logger.info("Collecting teams data...")
            teams = await self.collector.collect_teams_data()
            collected_data['teams'] = teams
            if self.config['save_to_files']:
                self.save_data(teams, f"teams_{season}")
            
            # 2. Collect schedule data
            logger.info(f"Collecting schedule for {season}...")
            schedule = await self.collector.collect_schedule(season)
            collected_data['schedule'] = schedule
            if self.config['save_to_files']:
                self.save_data(schedule, f"schedule_{season}")
            
            # 3. Collect game stats for completed games
            logger.info("Collecting game statistics...")
            game_stats = []
            completed_games = [game for game in schedule 
                             if game.get('home_points') is not None and game.get('away_points') is not None]
            
            # Limit to first 50 games to avoid rate limits in testing
            for i, game in enumerate(completed_games[:50]):
                if game.get('id'):
                    logger.info(f"Collecting stats for game {i+1}/{min(50, len(completed_games))}: {game['id']}")
                    stats = await self.collector.collect_game_stats(game['id'])
                    if stats:
                        stats['game_id'] = game['id']
                        stats['home_team'] = game['home_team']
                        stats['away_team'] = game['away_team']
                        game_stats.append(stats)
                    
                    # Add small delay to be respectful to API
                    await asyncio.sleep(0.5)
            
            collected_data['game_stats'] = game_stats
            if self.config['save_to_files']:
                self.save_data(game_stats, f"game_stats_{season}")
            
            # 4. Collect betting lines (current season only to avoid hitting limits)
            if season == self.config['current_season']:
                logger.info("Collecting betting lines...")
                betting_lines = await self.collector.collect_betting_lines(season)
                collected_data['betting_lines'] = betting_lines
                if self.config['save_to_files']:
                    self.save_data(betting_lines, f"betting_lines_{season}")
            
            logger.info(f"Data collection completed for {season}")
            return collected_data
            
        except Exception as e:
            logger.error(f"Error collecting data for {season}: {e}")
            return collected_data
    
    async def run_full_ingestion(self) -> None:
        """Run the complete data ingestion pipeline."""
        logger.info("Starting full data ingestion pipeline...")
        
        all_collected_data = {}
        
        # Collect data for each specified season
        for season in self.config['seasons_to_collect']:
            season_data = await self.collect_all_data(season)
            all_collected_data[season] = season_data
            
            # Add delay between seasons to be respectful to APIs
            await asyncio.sleep(2)
        
        # Save summary report
        summary = self.generate_summary_report(all_collected_data)
        self.save_data(summary, "collection_summary")
        
        logger.info("Full data ingestion pipeline completed!")
    
    def generate_summary_report(self, data: Dict) -> Dict:
        """Generate a summary report of collected data."""
        summary = {
            'collection_timestamp': datetime.now().isoformat(),
            'seasons_collected': list(data.keys()),
            'summary_by_season': {}
        }
        
        for season, season_data in data.items():
            season_summary = {
                'teams_count': len(season_data.get('teams', [])),
                'games_count': len(season_data.get('schedule', [])),
                'game_stats_count': len(season_data.get('game_stats', [])),
                'betting_lines_count': len(season_data.get('betting_lines', []))
            }
            summary['summary_by_season'][season] = season_summary
        
        return summary

def check_prerequisites() -> bool:
    """Check if all prerequisites are met."""
    config = get_config()
    
    # Check API keys
    if config['cfbd_api_key'] == 'your_cfbd_api_key_here':
        print("❌ CFBD API key not configured")
        print(API_SETUP_INSTRUCTIONS)
        return False
    
    if config['odds_api_key'] == 'your_odds_api_key_here':
        print("❌ Odds API key not configured")
        print(API_SETUP_INSTRUCTIONS)
        return False
    
    # Check Redis connection
    try:
        import redis
        r = redis.Redis(host=config['redis_host'], port=config['redis_port'])
        r.ping()
        print("✅ Redis connection successful")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        print("Please make sure Redis is running (redis-server)")
        return False
    
    print("✅ All prerequisites met!")
    return True

async def main():
    """Main entry point for the data ingestion script."""
    print("NCAA Football Data Ingestion Pipeline")
    print("=" * 40)
    
    # Check prerequisites
    if not check_prerequisites():
        print("\nPlease fix the issues above and try again.")
        return
    
    # Load configuration
    config = get_config()
    
    # Create and run pipeline
    pipeline = DataIngestionPipeline(config)
    
    # You can choose to run different parts:
    choice = input("\nWhat would you like to do?\n1. Collect current season data\n2. Run full ingestion\n3. Just collect teams\nEnter choice (1-3): ")
    
    if choice == "1":
        await pipeline.collect_all_data(config['current_season'])
    elif choice == "2":
        await pipeline.run_full_ingestion()
    elif choice == "3":
        teams = await pipeline.collector.collect_teams_data()
        pipeline.save_data(teams, "teams_current")
    else:
        print("Invalid choice. Running current season collection...")
        await pipeline.collect_all_data(config['current_season'])

if __name__ == "__main__":
    asyncio.run(main())