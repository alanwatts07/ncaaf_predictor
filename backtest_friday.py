#!/usr/bin/env python3
"""
BACKTEST: Friday Games Validation

This script tests how well our prediction system would have performed
on actual Friday games from last week. We'll ignore the scores and just
predict the outcomes to validate model accuracy.

Actual Results from Friday:
- Tarleton State at Army: Army won 30–27 (2OT)
- Kennesaw State at Wake Forest: Wake Forest won 10–9  
- Bethune-Cookman at FIU: FIU won 42–9
- Appalachian State vs Charlotte: App State won 34–11
- Western Michigan at Michigan State: Michigan State won 23–6
- Western Illinois at Illinois: Illinois won 52–3
- Wagner at Kansas: Kansas won 46–7
- Georgia Tech at Colorado: Georgia Tech won 27–20
- Auburn at Baylor: Auburn won 38–24
- UNLV at Sam Houston: UNLV won 38–21
- Central Michigan at San Jose State: Central Michigan won 16–14
"""

import sys
import json
from datetime import datetime
from pathlib import Path

sys.path.append('src')

from models.prediction_head_clean import GamePredictor
from simulation.monte_carlo import MonteCarloSimulator
from utils.team_name_mapping import normalize_team_name, create_team_name_mapping

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FridayBacktest:
    """Backtest system for Friday's actual games."""
    
    def __init__(self):
        self.predictor = None
        self.simulator = None
        self.team_mapping = create_team_name_mapping()
        
        # Actual Friday results (away_team, home_team, winner, actual_score)
        self.actual_games = [
            {
                'away_team': 'Tarleton State',  # FCS team
                'home_team': 'Army Black Knights', 
                'actual_winner': 'Army',
                'actual_score': '30-27 (2OT)',
                'notes': 'Double overtime'
            },
            {
                'away_team': 'Kennesaw State',
                'home_team': 'Wake Forest Demon Deacons',
                'actual_winner': 'Wake Forest', 
                'actual_score': '10-9',
                'notes': 'Low-scoring defensive game'
            },
            {
                'away_team': 'Bethune-Cookman',  # FCS team
                'home_team': 'FIU Panthers',
                'actual_winner': 'FIU',
                'actual_score': '42-9', 
                'notes': 'Blowout'
            },
            {
                'away_team': 'Appalachian State Mountaineers',
                'home_team': 'Charlotte 49ers',
                'actual_winner': 'Appalachian State',
                'actual_score': '34-11',
                'notes': 'Neutral site in Charlotte'
            },
            {
                'away_team': 'Western Michigan Broncos',
                'home_team': 'Michigan State Spartans',
                'actual_winner': 'Michigan State',
                'actual_score': '23-6',
                'notes': 'Big Ten vs MAC'
            },
            {
                'away_team': 'Western Illinois Leathernecks',  # FCS team
                'home_team': 'Illinois Fighting Illini',
                'actual_winner': 'Illinois',
                'actual_score': '52-3',
                'notes': 'FBS vs FCS blowout'
            },
            {
                'away_team': 'Wagner Seahawks',  # FCS team
                'home_team': 'Kansas Jayhawks',
                'actual_winner': 'Kansas',
                'actual_score': '46-7',
                'notes': 'FBS vs FCS'
            },
            {
                'away_team': 'Georgia Tech Yellow Jackets',
                'home_team': 'Colorado Buffaloes',
                'actual_winner': 'Georgia Tech',
                'actual_score': '27-20',
                'notes': 'Road upset'
            },
            {
                'away_team': 'Auburn Tigers',
                'home_team': 'Baylor Bears',
                'actual_winner': 'Auburn',
                'actual_score': '38-24',
                'notes': 'SEC at Big 12'
            },
            {
                'away_team': 'UNLV Rebels',
                'home_team': 'Sam Houston Bearkats',
                'actual_winner': 'UNLV',
                'actual_score': '38-21',
                'notes': 'Road win'
            },
            {
                'away_team': 'Central Michigan Chippewas',
                'home_team': 'San Jose State Spartans',
                'actual_winner': 'Central Michigan',
                'actual_score': '16-14',
                'notes': 'Close road win'
            }
        ]
        
    def initialize_system(self):
        """Load the trained models."""
        print("🏈 FRIDAY GAMES BACKTEST")
        print("=" * 40)
        print("Loading prediction system...")
        
        try:
            # Load trained prediction model
            self.predictor = GamePredictor()
            self.predictor.load_model('data/models/prediction_head.pkl')
            
            # Initialize simulator
            self.simulator = MonteCarloSimulator(self.predictor, n_simulations=1000)
            
            print(f"✅ System loaded with {len(self.predictor.team_embeddings)} teams")
            
        except Exception as e:
            print(f"❌ Failed to load system: {e}")
            raise
    
    def run_backtest(self):
        """Run predictions on Friday's actual games."""
        
        print(f"\n🎯 BACKTESTING {len(self.actual_games)} FRIDAY GAMES")
        print("=" * 50)
        
        results = []
        correct_predictions = 0
        total_predictions = 0
        
        for i, game in enumerate(self.actual_games, 1):
            print(f"\n📊 Game {i}: {game['away_team']} @ {game['home_team']}")
            print(f"   Actual Result: {game['actual_winner']} won {game['actual_score']}")
            
            try:
                # Normalize team names
                away_team = normalize_team_name(game['away_team'], self.team_mapping)
                home_team = normalize_team_name(game['home_team'], self.team_mapping)
                
                print(f"   Normalized: {away_team} @ {home_team}")
                
                # Get prediction
                prediction = self.simulator.simulate_game(away_team, home_team)
                
                # Determine predicted winner
                if prediction.predicted_spread > 0:
                    predicted_winner = prediction.home_team
                    predicted_winner_full = game['home_team'].split()[-1]  # Get just team name
                else:
                    predicted_winner = prediction.away_team  
                    predicted_winner_full = game['away_team'].split()[-1]  # Get just team name
                
                # Check if prediction was correct
                actual_winner_short = game['actual_winner']
                predicted_correct = (actual_winner_short.lower() in predicted_winner.lower() or 
                                   predicted_winner.lower() in actual_winner_short.lower())
                
                total_predictions += 1
                if predicted_correct:
                    correct_predictions += 1
                    result_symbol = "✅"
                else:
                    result_symbol = "❌"
                
                # Store detailed results
                game_result = {
                    'game': game,
                    'prediction': prediction,
                    'predicted_winner': predicted_winner,
                    'actual_winner': actual_winner_short,
                    'correct': predicted_correct,
                    'confidence': abs(prediction.predicted_spread),
                    'win_probability': prediction.home_win_prob if prediction.predicted_spread > 0 else (1 - prediction.home_win_prob)
                }
                results.append(game_result)
                
                # Display results
                print(f"   Predicted: {predicted_winner} by {abs(prediction.predicted_spread):.1f}")
                print(f"   Win Prob: {game_result['win_probability']:.1%}")
                print(f"   Confidence Interval: [{prediction.confidence_interval[0]:.1f}, {prediction.confidence_interval[1]:.1f}]")
                print(f"   Result: {result_symbol} {'CORRECT' if predicted_correct else 'INCORRECT'}")
                
            except Exception as e:
                print(f"   ⚠️  Could not predict: {e}")
                continue
        
        # Calculate overall accuracy
        if total_predictions > 0:
            accuracy = correct_predictions / total_predictions * 100
        else:
            accuracy = 0
            
        print(f"\n📈 BACKTEST RESULTS")
        print("=" * 25)
        print(f"Games Predicted: {total_predictions}")
        print(f"Correct Predictions: {correct_predictions}")
        print(f"Overall Accuracy: {accuracy:.1f}%")
        
        return results, accuracy
    
    def generate_backtest_report(self, results, accuracy):
        """Generate detailed backtest report."""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = f"reports/backtest_friday_{timestamp}.txt"
        
        report_lines = []
        report_lines.append("FRIDAY GAMES BACKTEST REPORT")
        report_lines.append("=" * 40)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Overall Accuracy: {accuracy:.1f}%")
        report_lines.append("")
        
        # Detailed game results
        report_lines.append("GAME-BY-GAME RESULTS:")
        report_lines.append("-" * 25)
        
        for i, result in enumerate(results, 1):
            game = result['game']
            pred = result['prediction']
            
            status = "✅ CORRECT" if result['correct'] else "❌ INCORRECT"
            
            report_lines.append(f"\n{i}. {game['away_team']} @ {game['home_team']}")
            report_lines.append(f"   Actual: {game['actual_winner']} won {game['actual_score']}")
            report_lines.append(f"   Predicted: {result['predicted_winner']} by {abs(pred.predicted_spread):.1f}")
            report_lines.append(f"   Win Probability: {result['win_probability']:.1%}")
            report_lines.append(f"   Status: {status}")
            if game.get('notes'):
                report_lines.append(f"   Notes: {game['notes']}")
        
        # Analysis
        report_lines.append(f"\n\nANALYSIS:")
        report_lines.append("-" * 10)
        
        # Correct predictions
        correct_games = [r for r in results if r['correct']]
        if correct_games:
            report_lines.append(f"Correctly predicted {len(correct_games)} games:")
            for r in correct_games:
                report_lines.append(f"  - {r['game']['actual_winner']} over {r['game']['away_team'] if r['game']['actual_winner'] != r['game']['away_team'].split()[-1] else r['game']['home_team']}")
        
        # Incorrect predictions
        incorrect_games = [r for r in results if not r['correct']]
        if incorrect_games:
            report_lines.append(f"\nMissed {len(incorrect_games)} predictions:")
            for r in incorrect_games:
                report_lines.append(f"  - Predicted {r['predicted_winner']}, but {r['game']['actual_winner']} won")
        
        report_lines.append(f"\nModel shows {accuracy:.1f}% accuracy on these Friday games.")
        report_lines.append("This validates the prediction system's performance on real game outcomes.")
        
        # Save report
        Path("reports").mkdir(exist_ok=True)
        with open(report_path, 'w') as f:
            f.write('\n'.join(report_lines))
        
        print(f"\n📋 Detailed report saved to: {report_path}")
        
        return '\n'.join(report_lines)

def main():
    """Run the Friday games backtest."""
    
    try:
        # Initialize backtest
        backtest = FridayBacktest()
        backtest.initialize_system()
        
        # Run the backtest
        results, accuracy = backtest.run_backtest()
        
        # Generate report
        if results:
            report = backtest.generate_backtest_report(results, accuracy)
            
            # Show summary
            print(f"\n🎉 BACKTEST COMPLETE!")
            print(f"Your model achieved {accuracy:.1f}% accuracy on Friday's games")
            
            if accuracy >= 70:
                print("🎯 Excellent performance! Model is well-calibrated.")
            elif accuracy >= 60:
                print("✅ Good performance! Model shows predictive power.")
            elif accuracy >= 50:
                print("📊 Moderate performance. Better than random chance.")
            else:
                print("⚠️  Model may need adjustment or more training data.")
                
            return results, accuracy
        else:
            print("❌ No valid predictions could be made")
            return None, 0
            
    except Exception as e:
        print(f"❌ Backtest failed: {e}")
        logger.error(f"Backtest error: {e}")
        return None, 0

if __name__ == "__main__":
    print("🏈 NCAA Football Prediction Backtest")
    print("Validating model performance on actual Friday game results\n")
    
    results, accuracy = main()
    
    if results:
        print(f"\n📊 VALIDATION COMPLETE")
        print(f"Model Accuracy: {accuracy:.1f}%")
        print("Check the detailed report for game-by-game analysis.")
    else:
        print("❌ Backtest could not be completed")