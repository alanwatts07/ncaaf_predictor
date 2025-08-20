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
        """Collect basic data for a given season."""
        logger.info(f"Starting basic data collection for {season} season...")
        
        collected_data = {}
        
        try:
            # 1. Collect teams data
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
            
            # 3. Collect basic team season stats
            logger.info("Collecting team season statistics...")
            team_stats = await self.collector.collect_team_season_stats(season)
            collected_data['team_season_stats'] = team_stats
            if self.config['save_to_files']:
                self.save_data(team_stats, f"team_season_stats_{season}")
            
            # 4. Collect betting lines (current season only)
            if season == self.config['current_season']:
                logger.info("Collecting betting lines...")
                betting_lines = await self.collector.collect_betting_lines(season)
                collected_data['betting_lines'] = betting_lines
                if self.config['save_to_files']:
                    self.save_data(betting_lines, f"betting_lines_{season}")
            
            logger.info(f"Basic data collection completed for {season}")
            return collected_data
            
        except Exception as e:
            logger.error(f"Error collecting basic data for {season}: {e}")
            return collected_data
    
    async def collect_comprehensive_data(self, season: int) -> Dict[str, Any]:
        """Collect comprehensive data including detailed player and team stats."""
        logger.info(f"Starting comprehensive data collection for {season} season...")
        
        collected_data = {}
        
        try:
            # 1. Basic team info
            logger.info("Collecting teams data...")
            teams = await self.collector.collect_teams_data()
            collected_data['teams'] = teams
            if self.config['save_to_files']:
                self.save_data(teams, f"teams_{season}")
            
            # 2. Game schedule
            logger.info(f"Collecting schedule for {season}...")
            schedule = await self.collector.collect_schedule(season)
            collected_data['schedule'] = schedule
            if self.config['save_to_files']:
                self.save_data(schedule, f"schedule_{season}")
            
            # 3. Team season statistics (comprehensive)
            logger.info("Collecting team season statistics...")
            team_stats = await self.collector.collect_team_season_stats(season)
            collected_data['team_season_stats'] = team_stats
            if self.config['save_to_files']:
                self.save_data(team_stats, f"team_season_stats_{season}")
            
            # 4. Advanced team statistics
            logger.info("Collecting advanced team statistics...")
            advanced_stats = await self.collector.collect_team_advanced_season_stats(season)
            collected_data['team_advanced_stats'] = advanced_stats
            if self.config['save_to_files']:
                self.save_data(advanced_stats, f"team_advanced_stats_{season}")
            
            # 5. Player season statistics by category
            player_categories = ['passing', 'rushing', 'receiving', 'defensive']
            for category in player_categories:
                logger.info(f"Collecting {category} player statistics...")
                player_stats = await self.collector.collect_player_season_stats(season, category)
                collected_data[f'player_{category}_stats'] = player_stats
                if self.config['save_to_files']:
                    self.save_data(player_stats, f"player_{category}_stats_{season}")
                await asyncio.sleep(1)  # Rate limiting
            
            # 6. Team talent ratings
            logger.info("Collecting team talent ratings...")
            talent_ratings = await self.collector.collect_team_talent_ratings(season)
            collected_data['talent_ratings'] = talent_ratings
            if self.config['save_to_files']:
                self.save_data(talent_ratings, f"talent_ratings_{season}")
            
            # 7. Recruiting data
            logger.info("Collecting recruiting data...")
            recruiting = await self.collector.collect_recruiting_data(season)
            collected_data['recruiting'] = recruiting
            if self.config['save_to_files']:
                self.save_data(recruiting, f"recruiting_{season}")
            
            # 8. Team game statistics (week by week)
            logger.info("Collecting weekly team game stats...")
            weekly_team_stats = []
            for week in range(1, 16):  # Regular season weeks
                logger.info(f"Collecting week {week} team stats...")
                week_stats = await self.collector.collect_team_game_stats(season, week)
                if week_stats:
                    weekly_team_stats.extend(week_stats)
                await asyncio.sleep(0.5)
            
            collected_data['weekly_team_stats'] = weekly_team_stats
            if self.config['save_to_files']:
                self.save_data(weekly_team_stats, f"weekly_team_stats_{season}")
            
            # 9. Player game statistics (sample - first 5 weeks)
            logger.info("Collecting sample player game stats...")
            player_game_stats = []
            for week in range(1, 6):  # Just first 5 weeks as sample
                logger.info(f"Collecting week {week} player game stats...")
                week_player_stats = await self.collector.collect_player_game_stats(season, week)
                if week_player_stats:
                    player_game_stats.extend(week_player_stats)
                await asyncio.sleep(1)
            
            collected_data['player_game_stats_sample'] = player_game_stats
            if self.config['save_to_files']:
                self.save_data(player_game_stats, f"player_game_stats_sample_{season}")
            
            logger.info(f"Comprehensive data collection completed for {season}")
            return collected_data
            
        except Exception as e:
            logger.error(f"Error in comprehensive collection for {season}: {e}")
            return collected_data
    
    async def collect_player_stats_only(self, season: int) -> Dict[str, Any]:
        """Collect only player statistics for a given season."""
        logger.info(f"Starting player stats collection for {season} season...")
        
        collected_data = {}
        
        try:
            # Collect all player stats categories
            player_categories = ['passing', 'rushing', 'receiving', 'defensive', 'kicking', 'punting', 'kickReturns', 'puntReturns', 'interceptions']
            
            for category in player_categories:
                logger.info(f"Collecting {category} player statistics...")
                player_stats = await self.collector.collect_player_season_stats(season, category)
                collected_data[f'player_{category}_stats'] = player_stats
                if self.config['save_to_files']:
                    self.save_data(player_stats, f"player_{category}_stats_{season}")
                await asyncio.sleep(1)  # Rate limiting
            
            logger.info(f"Player stats collection completed for {season}")
            return collected_data
            
        except Exception as e:
            logger.error(f"Error collecting player stats for {season}: {e}")
            return collected_data
    
    async def collect_detailed_game_data(self, season: int, max_weeks: int = 15) -> Dict[str, Any]:
        """Collect detailed game-by-game data for players and teams."""
        logger.info(f"Starting detailed game data collection for {season} season...")
        
        collected_data = {}
        
        try:
            # 1. Team game statistics (all weeks)
            logger.info("Collecting detailed team game stats...")
            weekly_team_stats = []
            for week in range(1, max_weeks + 1):
                logger.info(f"Collecting week {week} team stats...")
                week_stats = await self.collector.collect_team_game_stats(season, week)
                if week_stats:
                    weekly_team_stats.extend(week_stats)
                await asyncio.sleep(0.5)
            
            collected_data['weekly_team_stats'] = weekly_team_stats
            if self.config['save_to_files']:
                self.save_data(weekly_team_stats, f"detailed_weekly_team_stats_{season}")
            
            # 2. Player game statistics (all weeks - WARNING: This is A LOT of data)
            logger.info("Collecting detailed player game stats...")
            player_game_stats = []
            for week in range(1, max_weeks + 1):
                logger.info(f"Collecting week {week} player game stats...")
                week_player_stats = await self.collector.collect_player_game_stats(season, week)
                if week_player_stats:
                    player_game_stats.extend(week_player_stats)
                await asyncio.sleep(1)
            
            collected_data['player_game_stats'] = player_game_stats
            if self.config['save_to_files']:
                self.save_data(player_game_stats, f"detailed_player_game_stats_{season}")
            
            logger.info(f"Detailed game data collection completed for {season}")
            return collected_data
            
        except Exception as e:
            logger.error(f"Error collecting detailed game data for {season}: {e}")
            return collected_data
    
    async def run_full_ingestion(self) -> None:
        """Run the complete data ingestion pipeline for all configured seasons."""
        logger.info("Starting full data ingestion pipeline...")
        
        all_collected_data = {}
        
        # Collect data for each specified season
        for season in self.config['seasons_to_collect']:
            season_data = await self.collect_comprehensive_data(season)
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
                'team_season_stats_count': len(season_data.get('team_season_stats', [])),
                'team_advanced_stats_count': len(season_data.get('team_advanced_stats', [])),
                'player_passing_stats_count': len(season_data.get('player_passing_stats', [])),
                'player_rushing_stats_count': len(season_data.get('player_rushing_stats', [])),
                'player_receiving_stats_count': len(season_data.get('player_receiving_stats', [])),
                'player_defensive_stats_count': len(season_data.get('player_defensive_stats', [])),
                'talent_ratings_count': len(season_data.get('talent_ratings', [])),
                'recruiting_count': len(season_data.get('recruiting', [])),
                'weekly_team_stats_count': len(season_data.get('weekly_team_stats', [])),
                'player_game_stats_count': len(season_data.get('player_game_stats_sample', [])),
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
        print("⚠️  Odds API key not configured (optional for most collections)")
    
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
    
    print("✅ Prerequisites check completed!")
    return True

async def main():
    """Main entry point for the data ingestion script."""
    print("NCAA Football Data Ingestion Pipeline")
    print("=" * 50)
    
    # Check prerequisites
    if not check_prerequisites():
        print("\nPlease fix the issues above and try again.")
        return
    
    # Load configuration
    config = get_config()
    
    # Create pipeline
    pipeline = DataIngestionPipeline(config)
    
    print(f"\nConfigured for season: {config['current_season']}")
    print(f"Output directory: {config['output_dir']}")
    
    # Menu options
    print("\nData Collection Options:")
    print("1. Basic data collection (teams, schedule, basic stats)")
    print("2. Comprehensive data collection (ALL stats, recruiting, talent)")
    print("3. Player statistics only (all categories)")
    print("4. Detailed game-by-game data (WARNING: Very large dataset)")
    print("5. Teams data only (quick test)")
    print("6. Full multi-season ingestion")
    print("7. Custom selection")
    
    choice = input("\nEnter your choice (1-7): ").strip()
    
    start_time = datetime.now()
    
    try:
        if choice == "1":
            print(f"\n🚀 Starting basic data collection for {config['current_season']}...")
            await pipeline.collect_all_data(config['current_season'])
            
        elif choice == "2":
            print(f"\n🚀 Starting comprehensive data collection for {config['current_season']}...")
            print("⚠️  This will collect A LOT of data and may take a while!")
            confirm = input("Continue? (y/N): ").lower()
            if confirm == 'y':
                await pipeline.collect_comprehensive_data(config['current_season'])
            else:
                print("Cancelled.")
                return
                
        elif choice == "3":
            print(f"\n🚀 Starting player stats collection for {config['current_season']}...")
            await pipeline.collect_player_stats_only(config['current_season'])
            
        elif choice == "4":
            print(f"\n🚀 Starting detailed game data collection for {config['current_season']}...")
            print("⚠️  WARNING: This creates an extremely large dataset!")
            confirm = input("Continue? (y/N): ").lower()
            if confirm == 'y':
                max_weeks = int(input("Enter max weeks to collect (1-17, default 15): ") or "15")
                await pipeline.collect_detailed_game_data(config['current_season'], max_weeks)
            else:
                print("Cancelled.")
                return
                
        elif choice == "5":
            print("\n🚀 Starting teams data collection...")
            teams = await pipeline.collector.collect_teams_data()
            pipeline.save_data(teams, "teams_test")
            
        elif choice == "6":
            print(f"\n🚀 Starting full multi-season ingestion...")
            print(f"Seasons: {config['seasons_to_collect']}")
            confirm = input("This will take a very long time. Continue? (y/N): ").lower()
            if confirm == 'y':
                await pipeline.run_full_ingestion()
            else:
                print("Cancelled.")
                return
                
        elif choice == "7":
            print("\n🔧 Custom Selection:")
            season = int(input(f"Enter season year (default {config['current_season']}): ") or str(config['current_season']))
            
            print("\nWhat to collect:")
            collect_teams = input("Teams data? (y/N): ").lower() == 'y'
            collect_schedule = input("Schedule? (y/N): ").lower() == 'y'
            collect_team_stats = input("Team stats? (y/N): ").lower() == 'y'
            collect_player_stats = input("Player stats? (y/N): ").lower() == 'y'
            collect_game_data = input("Game-by-game data? (y/N): ").lower() == 'y'
            
            print(f"\n🚀 Starting custom collection for {season}...")
            # Implement custom collection logic here
            custom_data = {}
            
            if collect_teams:
                teams = await pipeline.collector.collect_teams_data()
                pipeline.save_data(teams, f"custom_teams_{season}")
                custom_data['teams'] = teams
            
            if collect_schedule:
                schedule = await pipeline.collector.collect_schedule(season)
                pipeline.save_data(schedule, f"custom_schedule_{season}")
                custom_data['schedule'] = schedule
            
            if collect_team_stats:
                team_stats = await pipeline.collector.collect_team_season_stats(season)
                pipeline.save_data(team_stats, f"custom_team_stats_{season}")
                custom_data['team_stats'] = team_stats
            
            if collect_player_stats:
                await pipeline.collect_player_stats_only(season)
            
            if collect_game_data:
                weeks = int(input("How many weeks? (1-17): ") or "15")
                await pipeline.collect_detailed_game_data(season, weeks)
            
        else:
            print("Invalid choice. Running basic collection...")
            await pipeline.collect_all_data(config['current_season'])
            
    except KeyboardInterrupt:
        print("\n\n❌ Collection interrupted by user.")
    except Exception as e:
        print(f"\n❌ Error during collection: {e}")
        logger.error(f"Collection error: {e}")
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n✅ Collection completed!")
    print(f"Duration: {duration}")
    print(f"Check {config['output_dir']} for collected data files.")
    print(f"Check data_collection.log for detailed logs.")

if __name__ == "__main__":
    asyncio.run(main())