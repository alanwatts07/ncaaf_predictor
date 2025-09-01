#!/usr/bin/env python3
"""
Quick test script to verify CFBD API connectivity and collect a small sample.
"""

import asyncio
import sys
import os
sys.path.append('src')

from data_pipeline.cfbd_collector import CFBDAdvancedCollector
from config import get_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_cfbd_connection():
    """Test CFBD API connection with a simple query."""
    
    print("🏈 Testing CFBD API Connection...")
    print("=" * 40)
    
    try:
        config = get_config()
        collector = CFBDAdvancedCollector(config)
        
        # Test 1: Try to fetch SP+ ratings for 2023 (should exist)
        print("\n🧪 Test 1: Fetching SP+ ratings for 2023...")
        sp_ratings = await collector.collect_sp_plus_ratings(2023)
        
        if sp_ratings:
            print(f"✅ Success! Retrieved {len(sp_ratings)} SP+ ratings")
            print(f"📊 Sample SP+ data: {sp_ratings[0] if sp_ratings else 'None'}")
            
            # Save sample
            collector.save_data(sp_ratings, "test_sp_plus_2023")
        else:
            print("❌ Failed to retrieve SP+ ratings")
            return False
        
        # Test 2: Try advanced team stats for 2023
        print("\n🧪 Test 2: Fetching advanced team stats for 2023...")
        adv_stats = await collector.collect_advanced_team_stats(2023)
        
        if adv_stats:
            print(f"✅ Success! Retrieved advanced stats for {len(adv_stats)} teams")
            print(f"📊 Sample advanced data keys: {list(adv_stats[0].keys()) if adv_stats else 'None'}")
            
            # Save sample  
            collector.save_data(adv_stats, "test_advanced_stats_2023")
        else:
            print("❌ Failed to retrieve advanced stats")
            return False
            
        # Test 3: Try opponent-adjusted processing
        print("\n🧪 Test 3: Processing opponent-adjusted stats...")
        opp_adj = await collector.collect_opponent_adjusted_stats(2023)
        
        if opp_adj:
            print(f"✅ Success! Processed opponent-adjusted stats for {len(opp_adj)} teams")
            print(f"📊 Sample adjusted data: {opp_adj[0] if opp_adj else 'None'}")
            
            # Save sample
            collector.save_data(opp_adj, "test_opponent_adjusted_2023")
        else:
            print("❌ Failed to process opponent-adjusted stats")
            return False
            
        print("\n🎉 All tests passed! CFBD integration is working.")
        print("📁 Check data/raw/ for test_*.json files")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_cfbd_connection())
    if success:
        print("\n✅ Ready to collect opponent-adjusted metrics!")
        print("🚀 Run: python3 collect_advanced_metrics.py")
    else:
        print("\n❌ Fix the issues above before proceeding")
        exit(1)