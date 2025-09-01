#!/usr/bin/env python3
"""
Quick fix to ensure 2024 CFBD data is properly saved for feature engineering.
"""

import json
import sys
import asyncio
sys.path.append('src')

from data_pipeline.cfbd_collector import CFBDAdvancedCollector
from config import get_config

async def ensure_2024_files():
    """Ensure all 2024 CFBD files are properly saved."""
    print("🔧 Ensuring 2024 CFBD files are available...")
    
    config = get_config()
    collector = CFBDAdvancedCollector(config)
    
    # Collect and save each dataset individually for 2024
    year = 2024
    
    datasets_to_collect = [
        ('sp_plus_ratings', collector.collect_sp_plus_ratings),
        ('fpi_ratings', collector.collect_fpi_ratings),
        ('ppa_team_stats', collector.collect_ppa_team_stats),
        ('talent_rankings', collector.collect_team_talent_rankings),
        ('recruiting_rankings', collector.collect_recruiting_rankings),
        ('opponent_adjusted_stats', collector.collect_opponent_adjusted_stats),
        ('advanced_team_stats', collector.collect_advanced_team_stats)
    ]
    
    for name, collect_func in datasets_to_collect:
        try:
            print(f"📊 Collecting {name}...")
            if name in ['ppa_team_stats', 'fpi_ratings', 'recruiting_rankings', 'opponent_adjusted_stats', 'advanced_team_stats']:
                data = await collect_func(year)
            else:
                data = await collect_func(year)
            
            if data:
                filename = f"{name}_{year}"
                collector.save_data(data, filename)
                print(f"✅ Saved {filename}.json with {len(data)} records")
            else:
                print(f"❌ No data for {name}")
                
            await asyncio.sleep(1)
            
        except Exception as e:
            print(f"❌ Error collecting {name}: {e}")
    
    print("✅ 2024 files should now be available for feature engineering")

if __name__ == "__main__":
    asyncio.run(ensure_2024_files())