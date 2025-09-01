#!/usr/bin/env python3
"""
Script to collect opponent-adjusted metrics and advanced statistics from CFBD API.

This script focuses on collecting the most predictive metrics for football
prediction models, including SP+ ratings and opponent-adjusted efficiency stats.
"""

import asyncio
import logging
import sys
import os
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.append('src')

from data_pipeline.cfbd_collector import CFBDAdvancedCollector
from config import get_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('advanced_data_collection.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class AdvancedMetricsPipeline:
    """Pipeline specifically for collecting opponent-adjusted and advanced metrics."""
    
    def __init__(self, config):
        self.config = config
        self.collector = CFBDAdvancedCollector(config)
        self.output_dir = Path(config['output_dir'])
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def save_data(self, data, filename):
        """Save data to JSON file."""
        self.collector.save_data(data, filename, str(self.output_dir))
    
    async def collect_current_season_advanced_data(self):
        """Collect advanced data for the current season (most important for predictions)."""
        current_year = self.config['current_season']
        logger.info(f"🎯 Collecting advanced metrics for {current_year} season...")
        
        # Collect comprehensive advanced data
        data = await self.collector.collect_comprehensive_advanced_data(current_year)
        
        # Save individual datasets
        for key, dataset in data.items():
            if dataset:  # Only save non-empty datasets
                self.save_data(dataset, f"{key}_{current_year}")
        
        # Save combined dataset
        summary = {
            'collection_timestamp': datetime.now().isoformat(),
            'year': current_year,
            'datasets_collected': {k: len(v) if isinstance(v, list) else len(v.keys()) for k, v in data.items() if v},
            'data': data
        }
        self.save_data(summary, f"advanced_metrics_complete_{current_year}")
        
        return data
    
    async def collect_historical_advanced_data(self, years: list):
        """Collect advanced data for historical years (for model training)."""
        logger.info(f"📊 Collecting historical advanced metrics for years: {years}")
        
        all_data = {}
        
        for year in years:
            logger.info(f"Processing {year}...")
            try:
                year_data = await self.collector.collect_comprehensive_advanced_data(year)
                all_data[year] = year_data
                
                # Save individual year data
                for key, dataset in year_data.items():
                    if dataset:
                        self.save_data(dataset, f"{key}_{year}")
                
                # Brief pause between years
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Error collecting data for {year}: {e}")
                continue
        
        # Save combined historical data
        historical_summary = {
            'collection_timestamp': datetime.now().isoformat(),
            'years_collected': years,
            'summary_by_year': {
                year: {k: len(v) if isinstance(v, list) else len(v.keys()) for k, v in data.items() if v}
                for year, data in all_data.items()
            },
            'data': all_data
        }
        self.save_data(historical_summary, "historical_advanced_metrics")
        
        return all_data
    
    async def collect_weekly_ppa_data(self, year: int, max_weeks: int = 15):
        """Collect week-by-week PPA data for more granular analysis."""
        logger.info(f"📈 Collecting weekly PPA data for {year}, weeks 1-{max_weeks}...")
        
        weekly_data = {}
        
        for week in range(1, max_weeks + 1):
            logger.info(f"Collecting week {week} PPA data...")
            try:
                week_ppa = await self.collector.collect_game_ppa_stats(year, week=week)
                if week_ppa:
                    weekly_data[f'week_{week}'] = week_ppa
                    self.save_data(week_ppa, f"ppa_games_week_{week}_{year}")
                
                await asyncio.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error collecting week {week} PPA data: {e}")
                continue
        
        # Save combined weekly data
        weekly_summary = {
            'collection_timestamp': datetime.now().isoformat(),
            'year': year,
            'weeks_collected': list(weekly_data.keys()),
            'total_games': sum(len(games) for games in weekly_data.values()),
            'data': weekly_data
        }
        self.save_data(weekly_summary, f"weekly_ppa_complete_{year}")
        
        return weekly_data

def check_cfbd_credentials():
    """Check if CFBD API credentials are properly configured."""
    config = get_config()
    
    if not config.get('cfbd_api_key'):
        logger.error("❌ CFBD API key not found!")
        logger.error("Please set your CFBD_API_KEY environment variable or update config.py")
        logger.error("You can get a free API key at: https://collegefootballdata.com/key")
        return False
    
    logger.info("✅ CFBD API key found")
    return True

async def main():
    """Main execution function."""
    print("🏈 NCAA Advanced Metrics Collector")
    print("=" * 50)
    print("This script collects opponent-adjusted metrics from CFBD API")
    print("These are the most important stats for prediction models!\n")
    
    # Check credentials
    if not check_cfbd_credentials():
        return
    
    # Load config
    config = get_config()
    
    # Create pipeline
    pipeline = AdvancedMetricsPipeline(config)
    
    print("📋 Collection Options:")
    print("1. Current season advanced metrics (2024) - RECOMMENDED")
    print("2. Historical advanced metrics (2020-2023) - For model training")
    print("3. Weekly PPA data (current season) - For granular analysis")
    print("4. Complete package (all above) - COMPREHENSIVE")
    print("5. Quick test (SP+ ratings only)")
    
    choice = input("\nEnter your choice (1-5): ").strip()
    
    start_time = datetime.now()
    
    try:
        if choice == "1":
            print(f"\n🎯 Collecting current season ({config['current_season']}) advanced metrics...")
            await pipeline.collect_current_season_advanced_data()
            
        elif choice == "2":
            print("\n📊 Collecting historical advanced metrics...")
            historical_years = [2020, 2021, 2022, 2023]  # Skip current year
            confirm = input(f"This will collect data for {historical_years}. Continue? (y/N): ")
            if confirm.lower() == 'y':
                await pipeline.collect_historical_advanced_data(historical_years)
            else:
                print("Cancelled.")
                return
                
        elif choice == "3":
            print(f"\n📈 Collecting weekly PPA data for {config['current_season']}...")
            weeks = int(input("How many weeks to collect (1-17, default 15): ") or "15")
            await pipeline.collect_weekly_ppa_data(config['current_season'], weeks)
            
        elif choice == "4":
            print("\n🚀 Collecting COMPLETE advanced metrics package...")
            confirm = input("This will take a while and use significant API calls. Continue? (y/N): ")
            if confirm.lower() == 'y':
                # Current season
                await pipeline.collect_current_season_advanced_data()
                await asyncio.sleep(2)
                
                # Historical data
                historical_years = [2020, 2021, 2022, 2023]
                await pipeline.collect_historical_advanced_data(historical_years)
                await asyncio.sleep(2)
                
                # Weekly PPA for current season
                await pipeline.collect_weekly_ppa_data(config['current_season'])
            else:
                print("Cancelled.")
                return
                
        elif choice == "5":
            print("\n⚡ Quick test - SP+ ratings only...")
            sp_ratings = await pipeline.collector.collect_sp_plus_ratings(config['current_season'])
            pipeline.save_data(sp_ratings, f"sp_plus_test_{config['current_season']}")
            print(f"Collected SP+ ratings for {len(sp_ratings)} teams")
            
        else:
            print("Invalid choice. Running current season collection...")
            await pipeline.collect_current_season_advanced_data()
            
    except KeyboardInterrupt:
        print("\n\n❌ Collection interrupted by user.")
    except Exception as e:
        print(f"\n❌ Error during collection: {e}")
        logger.error(f"Collection error: {e}")
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n✅ Collection completed!")
    print(f"⏱️  Duration: {duration}")
    print(f"📁 Check {config['output_dir']} for collected files")
    print(f"📋 Check advanced_data_collection.log for details")
    print("\n🎯 Key files to look for:")
    print("   - sp_plus_ratings_*.json (most important)")
    print("   - opponent_adjusted_stats_*.json (crucial)")
    print("   - advanced_team_stats_*.json (comprehensive)")

if __name__ == "__main__":
    asyncio.run(main())