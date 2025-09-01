#!/usr/bin/env python3
"""
COMPLETE NCAAF PREDICTION SYSTEM

This script demonstrates the full pipeline:
1. Load trained models (VME + Prediction Head)
2. Pull upcoming games (or use sample games)
3. Run Monte Carlo simulations
4. Identify best betting opportunities
5. Generate comprehensive analysis report

Run this to find next week's best betting opportunities!
"""

import sys
import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append('src')

from models.prediction_head_clean import GamePredictor
from simulation.monte_carlo import MonteCarloSimulator, generate_betting_report
from utils.team_name_mapping import normalize_team_name, create_team_name_mapping
from data_pipeline.cfbd_collector import CFBDAdvancedCollector
from config import get_config

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NCAAFPredictionSystem:
    """
    Complete NCAA Football Prediction System.
    
    Integrates all components for end-to-end game prediction and betting analysis.
    """
    
    def __init__(self):
        self.predictor = None
        self.simulator = None
        self.cfbd_collector = None
        self.is_initialized = False
        
    def initialize(self):
        """Initialize all system components."""
        print("🏈 NCAA Football Prediction System")
        print("=" * 50)
        print("Initializing system components...")
        
        try:
            # Load trained prediction model
            print("📊 Loading trained prediction model...")
            self.predictor = GamePredictor()
            self.predictor.load_model('data/models/prediction_head.pkl')
            print(f"   ✅ Model loaded with {len(self.predictor.team_embeddings)} teams")
            
            # Initialize Monte Carlo simulator
            print("🎲 Initializing Monte Carlo simulator...")
            self.simulator = MonteCarloSimulator(self.predictor, n_simulations=1000)
            print("   ✅ Simulator ready")
            
            # Initialize CFBD collector (for future live data)
            print("📡 Initializing CFBD collector...")
            config = get_config()
            self.cfbd_collector = CFBDAdvancedCollector(config)
            print("   ✅ Collector ready")
            
            self.is_initialized = True
            print("✅ System initialization complete!\n")
            
        except Exception as e:
            print(f"❌ System initialization failed: {e}")
            raise
    
    def get_sample_upcoming_games(self) -> list:
        """
        Get actual upcoming games for Friday, September 5, 2025.
        
        These are the real games scheduled for this Friday evening.
        """
        
        # REAL Friday, September 5, 2025 games
        upcoming_games = [
            {
                'home_team': 'Louisville Cardinals',
                'away_team': 'James Madison Dukes', 
                'date': '2025-09-05',
                'time': '7:00 PM ET',
                'week': 2,
                'tv': 'TBD'
            },
            {
                'home_team': 'Maryland Terrapins',
                'away_team': 'Northern Illinois Huskies',
                'date': '2025-09-05', 
                'time': '7:00 PM ET',
                'week': 2,
                'conference': 'Big Ten',
                'tv': 'TBD'
            },
            {
                'home_team': 'Northwestern Wildcats',
                'away_team': 'Western Illinois Leathernecks',
                'date': '2025-09-05',
                'time': '7:30 PM ET', 
                'week': 2,
                'conference': 'Big Ten',
                'tv': 'TBD'
            },
            {
                'home_team': 'Boise State Broncos',
                'away_team': 'Eastern Washington Eagles',
                'date': '2025-09-05',
                'time': '9:00 PM ET',
                'week': 2,
                'tv': 'TBD'
            }
        ]
        
        return upcoming_games
    
    async def fetch_live_upcoming_games(self, weeks_ahead: int = 1) -> list:
        """
        Fetch upcoming games from CFBD API.
        
        This is where you'd implement live data fetching for production use.
        """
        print(f"🔄 Fetching upcoming games ({weeks_ahead} weeks ahead)...")
        
        # Placeholder for CFBD API call
        # In production, you'd call something like:
        # games = await self.cfbd_collector.get_upcoming_games(weeks_ahead)
        
        # For now, use sample games
        games = self.get_sample_upcoming_games()
        
        print(f"   📅 Found {len(games)} upcoming games")
        return games
    
    def predict_games(self, games: list, detailed: bool = True) -> tuple:
        """
        Run predictions and simulations for a list of games.
        
        Args:
            games: List of game dictionaries
            detailed: Whether to run full Monte Carlo simulations
            
        Returns:
            (predictions, opportunities)
        """
        
        if not self.is_initialized:
            raise RuntimeError("System not initialized. Call initialize() first.")
        
        print(f"🎯 Running predictions for {len(games)} games...")
        
        # Run Monte Carlo simulations
        predictions = self.simulator.simulate_multiple_games(games)
        
        print(f"   ✅ Completed {len(predictions)} game simulations")
        
        # Identify betting opportunities
        print("💰 Identifying betting opportunities...")
        opportunities = self.simulator.identify_betting_opportunities(
            predictions, 
            confidence_threshold=0.6
        )
        
        high_conf = len([opp for opp in opportunities if opp.confidence >= 0.8])
        medium_conf = len([opp for opp in opportunities if 0.6 <= opp.confidence < 0.8])
        
        print(f"   📈 Found {len(opportunities)} total opportunities")
        print(f"   🎯 High confidence: {high_conf}")
        print(f"   📊 Medium confidence: {medium_conf}")
        
        return predictions, opportunities
    
    def generate_report(self, opportunities: list, save_file: bool = True) -> str:
        """Generate and optionally save betting analysis report."""
        
        print("📋 Generating betting analysis report...")
        
        report = generate_betting_report(opportunities)
        
        if save_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_path = f"reports/ncaaf_betting_analysis_{timestamp}.txt"
            
            Path("reports").mkdir(exist_ok=True)
            with open(report_path, 'w') as f:
                f.write(report)
            
            print(f"   💾 Report saved to: {report_path}")
        
        return report
    
    def run_full_analysis(self, use_live_data: bool = False) -> dict:
        """
        Run complete end-to-end analysis.
        
        Args:
            use_live_data: Whether to attempt fetching live data from CFBD
            
        Returns:
            Dictionary with all results
        """
        
        if not self.is_initialized:
            self.initialize()
        
        print("\n🚀 RUNNING FULL GAME ANALYSIS")
        print("=" * 40)
        
        # Get upcoming games
        if use_live_data:
            # In production, this would fetch from CFBD API
            games = asyncio.run(self.fetch_live_upcoming_games())
        else:
            games = self.get_sample_upcoming_games()
            print(f"📋 Using {len(games)} sample games for demonstration")
        
        # Run predictions
        predictions, opportunities = self.predict_games(games)
        
        # Generate report
        report = self.generate_report(opportunities)
        
        # Summary stats
        total_games = len(predictions)
        total_opportunities = len(opportunities)
        high_confidence = len([opp for opp in opportunities if opp.confidence >= 0.8])
        
        results = {
            'total_games_analyzed': total_games,
            'total_opportunities': total_opportunities,
            'high_confidence_opportunities': high_confidence,
            'predictions': predictions,
            'opportunities': opportunities,
            'report': report
        }
        
        print("\n📊 ANALYSIS COMPLETE")
        print("=" * 20)
        print(f"Games Analyzed: {total_games}")
        print(f"Betting Opportunities: {total_opportunities}")
        print(f"High Confidence Bets: {high_confidence}")
        print("\nSee full report above for detailed analysis.")
        
        return results

def main():
    """Main execution function."""
    
    try:
        # Create and initialize system
        system = NCAAFPredictionSystem()
        
        # Run full analysis
        results = system.run_full_analysis(use_live_data=False)
        
        # Display the report
        print("\n" + "=" * 60)
        print("BETTING ANALYSIS REPORT")
        print("=" * 60)
        print(results['report'])
        
        print(f"\n✅ SUCCESS! System analyzed {results['total_games_analyzed']} games")
        print(f"💰 Found {results['high_confidence_opportunities']} high-confidence opportunities")
        print("\n🎯 SYSTEM READY FOR WEEKLY PREDICTIONS!")
        
        return results
        
    except Exception as e:
        print(f"\n❌ SYSTEM ERROR: {e}")
        logger.error(f"System error: {e}")
        return None

if __name__ == "__main__":
    print("🏈 NCAA Football Prediction System - Complete Pipeline")
    print("This system uses opponent-adjusted metrics and Monte Carlo simulation")
    print("to identify the best betting opportunities in college football.\n")
    
    results = main()
    
    if results:
        print("\n" + "="*60)
        print("NEXT STEPS:")
        print("="*60)
        print("1. 📊 Review the betting opportunities above")
        print("2. 🔍 Do additional research on recommended games") 
        print("3. 📈 Compare predictions with sportsbook lines")
        print("4. 💰 Make informed betting decisions")
        print("\n⚠️  DISCLAIMER: This is for educational purposes only.")
        print("   Always do your own research and bet responsibly!")