# src/simulation/monte_carlo.py
import numpy as np
import pandas as pd
import json
import asyncio
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import logging
import sys
sys.path.append('src')

from models.prediction_head_clean import GamePredictor
from utils.team_name_mapping import normalize_team_name, create_team_name_mapping

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class GamePrediction:
    """Data class for game prediction results."""
    home_team: str
    away_team: str
    predicted_spread: float
    predicted_total: float
    home_win_prob: float
    confidence_interval: Tuple[float, float]  # (lower, upper) for spread
    total_confidence_interval: Tuple[float, float]  # (lower, upper) for total
    simulation_std: float
    game_date: Optional[str] = None
    neutral_site: bool = False

@dataclass
class BettingOpportunity:
    """Data class for identified betting opportunities."""
    game: GamePrediction
    bet_type: str  # 'spread', 'total_over', 'total_under', 'moneyline'
    predicted_value: float
    confidence: float  # 0-1 scale
    market_line: Optional[float] = None  # If available
    edge: Optional[float] = None  # Expected edge vs market
    recommendation: str = "ANALYZE"  # "BET", "AVOID", "ANALYZE"

class MonteCarloSimulator:
    """
    Monte Carlo simulation engine for football game predictions.
    
    Runs multiple simulations per game to generate confidence intervals
    and identify high-value betting opportunities.
    """
    
    def __init__(self, predictor: GamePredictor, n_simulations: int = 1000):
        """
        Initialize the Monte Carlo simulator.
        
        Args:
            predictor: Trained GamePredictor model
            n_simulations: Number of simulations per game
        """
        self.predictor = predictor
        self.n_simulations = n_simulations
        self.team_mapping = create_team_name_mapping()
        
    def simulate_game(self, 
                     home_team: str, 
                     away_team: str, 
                     neutral_site: bool = False,
                     spread_variance: float = 12.0,
                     total_variance: float = 8.0) -> GamePrediction:
        """
        Run Monte Carlo simulation for a single game.
        
        Args:
            home_team: Home team name (short format)
            away_team: Away team name (short format) 
            neutral_site: Whether game is at neutral site
            spread_variance: Variance in spread predictions
            total_variance: Variance in total predictions
            
        Returns:
            GamePrediction with simulation results
        """
        
        # Get base prediction
        base_pred = self.predictor.predict_game(home_team, away_team, neutral_site=neutral_site)
        
        # Run simulations
        spread_sims = []
        total_sims = []
        
        for _ in range(self.n_simulations):
            # Add noise to base predictions
            spread_sim = np.random.normal(base_pred['spread'], spread_variance)
            total_sim = np.random.normal(base_pred['total'], total_variance)
            
            spread_sims.append(spread_sim)
            total_sims.append(total_sim)
        
        # Calculate statistics
        spread_mean = np.mean(spread_sims)
        total_mean = np.mean(total_sims)
        spread_std = np.std(spread_sims)
        total_std = np.std(total_sims)
        
        # Calculate confidence intervals (95%)
        spread_ci = (
            np.percentile(spread_sims, 2.5),
            np.percentile(spread_sims, 97.5)
        )
        
        total_ci = (
            np.percentile(total_sims, 2.5),
            np.percentile(total_sims, 97.5)
        )
        
        # Calculate win probability from simulations
        home_wins = sum(1 for s in spread_sims if s > 0) / len(spread_sims)
        
        return GamePrediction(
            home_team=home_team,
            away_team=away_team,
            predicted_spread=round(spread_mean, 1),
            predicted_total=round(total_mean, 1),
            home_win_prob=round(home_wins, 3),
            confidence_interval=spread_ci,
            total_confidence_interval=total_ci,
            simulation_std=round(spread_std, 2),
            neutral_site=neutral_site
        )
    
    def simulate_multiple_games(self, games: List[Dict]) -> List[GamePrediction]:
        """
        Run Monte Carlo simulations for multiple games.
        
        Args:
            games: List of game dictionaries with 'home_team', 'away_team', optional 'neutral_site'
        """
        predictions = []
        
        logger.info(f"Running Monte Carlo simulations for {len(games)} games...")
        
        for i, game in enumerate(games):
            try:
                # Normalize team names
                home_team = normalize_team_name(game['home_team'], self.team_mapping)
                away_team = normalize_team_name(game['away_team'], self.team_mapping)
                
                pred = self.simulate_game(
                    home_team, 
                    away_team, 
                    neutral_site=game.get('neutral_site', False)
                )
                
                pred.game_date = game.get('date', None)
                predictions.append(pred)
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Completed {i + 1}/{len(games)} simulations")
                    
            except Exception as e:
                logger.warning(f"Failed to simulate {game}: {e}")
                continue
        
        logger.info(f"Completed {len(predictions)} game simulations")
        return predictions
    
    def identify_betting_opportunities(self, 
                                    predictions: List[GamePrediction],
                                    confidence_threshold: float = 0.7) -> List[BettingOpportunity]:
        """
        Identify high-confidence betting opportunities from predictions.
        
        Args:
            predictions: List of game predictions
            confidence_threshold: Minimum confidence for recommendations
        """
        opportunities = []
        
        for pred in predictions:
            # Spread opportunities
            spread_confidence = 1.0 / (1.0 + pred.simulation_std / 10.0)  # Higher std = lower confidence
            
            if spread_confidence >= confidence_threshold:
                if abs(pred.predicted_spread) >= 3.0:  # Only strong spread predictions
                    opp = BettingOpportunity(
                        game=pred,
                        bet_type='spread',
                        predicted_value=pred.predicted_spread,
                        confidence=spread_confidence,
                        recommendation="ANALYZE" if spread_confidence < 0.8 else "BET"
                    )
                    opportunities.append(opp)
            
            # Total opportunities (more conservative)
            total_range = pred.total_confidence_interval[1] - pred.total_confidence_interval[0]
            total_confidence = 1.0 / (1.0 + total_range / 20.0)  # Tighter range = higher confidence
            
            if total_confidence >= confidence_threshold:
                opp = BettingOpportunity(
                    game=pred,
                    bet_type='total_analysis',
                    predicted_value=pred.predicted_total,
                    confidence=total_confidence,
                    recommendation="ANALYZE"
                )
                opportunities.append(opp)
            
            # Moneyline opportunities for strong favorites/underdogs
            if pred.home_win_prob >= 0.7 or pred.home_win_prob <= 0.3:
                ml_confidence = abs(pred.home_win_prob - 0.5) * 2  # Scale to 0-1
                
                opp = BettingOpportunity(
                    game=pred,
                    bet_type='moneyline',
                    predicted_value=pred.home_win_prob,
                    confidence=ml_confidence,
                    recommendation="ANALYZE" if ml_confidence < 0.6 else "BET"
                )
                opportunities.append(opp)
        
        # Sort by confidence
        opportunities.sort(key=lambda x: x.confidence, reverse=True)
        
        logger.info(f"Identified {len(opportunities)} betting opportunities")
        return opportunities

async def fetch_upcoming_games(cfbd_api_key: str, weeks_ahead: int = 1) -> List[Dict]:
    """
    Fetch upcoming games from CFBD API.
    
    Args:
        cfbd_api_key: CFBD API key
        weeks_ahead: How many weeks ahead to look
        
    Returns:
        List of upcoming games
    """
    import aiohttp
    
    # This is a placeholder - in a real implementation you'd call CFBD API
    # For now, let's create some sample upcoming games for demonstration
    
    sample_games = [
        {
            'home_team': 'Georgia Bulldogs',
            'away_team': 'Alabama Crimson Tide',
            'date': '2024-12-07',
            'neutral_site': True,
            'week': 15
        },
        {
            'home_team': 'Ohio State Buckeyes',
            'away_team': 'Michigan Wolverines', 
            'date': '2024-11-30',
            'neutral_site': False,
            'week': 14
        },
        {
            'home_team': 'Oregon Ducks',
            'away_team': 'Washington Huskies',
            'date': '2024-11-30',
            'neutral_site': False,
            'week': 14
        },
        {
            'home_team': 'Texas Longhorns',
            'away_team': 'Texas A&M Aggies',
            'date': '2024-11-30',
            'neutral_site': False,
            'week': 14
        },
        {
            'home_team': 'Notre Dame Fighting Irish',
            'away_team': 'USC Trojans',
            'date': '2024-11-30',
            'neutral_site': False,
            'week': 14
        }
    ]
    
    logger.info(f"Fetched {len(sample_games)} upcoming games")
    return sample_games

def generate_betting_report(opportunities: List[BettingOpportunity]) -> str:
    """Generate a formatted betting report."""
    
    report = []
    report.append("COLLEGE FOOTBALL BETTING ANALYSIS")
    report.append("FRIDAY, SEPTEMBER 5, 2025 GAMES")
    report.append("=" * 50)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # Add game overview
    report.append("GAMES ANALYZED:")
    report.append("-" * 20)
    games_analyzed = set()
    for opp in opportunities:
        game_str = f"{opp.game.away_team} @ {opp.game.home_team}"
        if game_str not in games_analyzed:
            games_analyzed.add(game_str)
            report.append(f"  {game_str}")
            report.append(f"    Predicted Spread: {opp.game.home_team} {opp.game.predicted_spread:+.1f}")
            report.append(f"    Predicted Total: {opp.game.predicted_total:.1f}")
            report.append(f"    {opp.game.home_team} Win Probability: {opp.game.home_win_prob:.1%}")
    report.append("")
    
    # High confidence bets
    high_conf_bets = [opp for opp in opportunities if opp.confidence >= 0.8]
    if high_conf_bets:
        report.append("HIGH CONFIDENCE OPPORTUNITIES:")
        report.append("-" * 30)
        for opp in high_conf_bets[:5]:  # Top 5
            game = opp.game
            report.append(f"{game.home_team} vs {game.away_team}")
            report.append(f"  Bet Type: {opp.bet_type}")
            report.append(f"  Prediction: {opp.predicted_value}")
            report.append(f"  Confidence: {opp.confidence:.1%}")
            report.append(f"  Recommendation: {opp.recommendation}")
            if opp.bet_type == 'spread':
                report.append(f"  Spread CI: [{game.confidence_interval[0]:.1f}, {game.confidence_interval[1]:.1f}]")
            report.append("")
    
    # Analysis opportunities  
    analysis_bets = [opp for opp in opportunities if 0.6 <= opp.confidence < 0.8]
    if analysis_bets:
        report.append("ANALYSIS OPPORTUNITIES:")
        report.append("-" * 25)
        for opp in analysis_bets[:10]:  # Top 10
            game = opp.game
            report.append(f"{game.home_team} vs {game.away_team}")
            report.append(f"  Type: {opp.bet_type} | Confidence: {opp.confidence:.1%}")
            if opp.bet_type == 'spread':
                report.append(f"  Spread: {game.predicted_spread} [{game.confidence_interval[0]:.1f}, {game.confidence_interval[1]:.1f}]")
            elif opp.bet_type == 'total_analysis':
                report.append(f"  Total: {game.predicted_total} [{game.total_confidence_interval[0]:.1f}, {game.total_confidence_interval[1]:.1f}]")
            else:
                report.append(f"  Win Prob: {game.home_win_prob:.1%}")
            report.append("")
    
    report.append("DISCLAIMER: This is for educational purposes only.")
    report.append("Always do your own research before making any wagers.")
    
    return "\n".join(report)

def main():
    """Main Monte Carlo simulation script."""
    print("Step 5: Monte Carlo Simulation Engine")
    print("=" * 40)
    
    try:
        # Load trained prediction model
        print("Loading trained prediction model...")
        predictor = GamePredictor()
        predictor.load_model('data/models/prediction_head.pkl')
        
        # Initialize simulator
        print("Initializing Monte Carlo simulator...")
        simulator = MonteCarloSimulator(predictor, n_simulations=1000)
        
        # Get upcoming games (in real implementation, would fetch from API)
        print("Fetching upcoming games...")
        upcoming_games = [
            {'home_team': 'Georgia Bulldogs', 'away_team': 'Alabama Crimson Tide', 'neutral_site': True},
            {'home_team': 'Ohio State Buckeyes', 'away_team': 'Michigan Wolverines'},
            {'home_team': 'Oregon Ducks', 'away_team': 'Washington Huskies'},
            {'home_team': 'Texas Longhorns', 'away_team': 'Texas A&M Aggies'},
            {'home_team': 'Notre Dame Fighting Irish', 'away_team': 'USC Trojans'},
            {'home_team': 'Clemson Tigers', 'away_team': 'South Carolina Gamecocks'},
            {'home_team': 'Florida State Seminoles', 'away_team': 'Florida Gators'},
            {'home_team': 'Auburn Tigers', 'away_team': 'Alabama Crimson Tide'}
        ]
        
        # Run simulations
        print(f"Running Monte Carlo simulations for {len(upcoming_games)} games...")
        predictions = simulator.simulate_multiple_games(upcoming_games)
        
        # Identify betting opportunities
        print("Identifying betting opportunities...")
        opportunities = simulator.identify_betting_opportunities(predictions, confidence_threshold=0.6)
        
        # Generate report
        print("Generating betting analysis report...")
        report = generate_betting_report(opportunities)
        
        # Save report
        report_path = f"reports/betting_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        Path("reports").mkdir(exist_ok=True)
        
        with open(report_path, 'w') as f:
            f.write(report)
        
        print(f"\nBetting Analysis Report:")
        print("=" * 25)
        print(report)
        
        print(f"\nReport saved to: {report_path}")
        print("\nStep 5 Complete! System ready for game predictions!")
        
        return simulator, predictions, opportunities
        
    except Exception as e:
        print(f"Error in Monte Carlo simulation: {e}")
        logger.error(f"Simulation error: {e}")
        return None

if __name__ == "__main__":
    main()