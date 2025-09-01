#!/usr/bin/env python3
"""
Step 1: Collect 2024 opponent-adjusted metrics for Monte Carlo simulations.
"""

import asyncio
import sys
import os
sys.path.append('src')

from data_pipeline.cfbd_collector import CFBDAdvancedCollector
from config import get_config
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def collect_2024_advanced_data():
    """Collect comprehensive 2024 data for prediction models."""
    
    print("🏈 Step 1: Collecting 2024 Opponent-Adjusted Data")
    print("=" * 55)
    
    config = get_config()
    collector = CFBDAdvancedCollector(config)
    
    year = 2024
    
    try:
        # Collect comprehensive advanced data
        print("🚀 Starting comprehensive data collection...")
        data = await collector.collect_comprehensive_advanced_data(year)
        
        print("\n📊 Collection Results:")
        for key, dataset in data.items():
            if dataset:
                count = len(dataset) if isinstance(dataset, list) else len(dataset.keys())
                print(f"  ✅ {key}: {count} records")
            else:
                print(f"  ❌ {key}: No data")
        
        # Save summary
        summary = {
            'year': year,
            'collection_status': 'completed',
            'datasets': {k: len(v) if isinstance(v, list) else len(v.keys()) for k, v in data.items() if v}
        }
        
        collector.save_data(summary, f"collection_summary_{year}")
        
        print(f"\n✅ 2024 data collection completed!")
        print(f"📁 Files saved to data/raw/")
        
        return data
        
    except Exception as e:
        print(f"❌ Error collecting 2024 data: {e}")
        logger.error(f"Collection error: {e}")
        return None

if __name__ == "__main__":
    result = asyncio.run(collect_2024_advanced_data())
    if result:
        print("\n🎯 Ready for Step 2: Feature Engineering!")
    else:
        print("❌ Fix collection issues before proceeding to Step 2")