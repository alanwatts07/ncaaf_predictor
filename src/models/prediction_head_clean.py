# src/models/prediction_head_clean.py
import numpy as np
import pandas as pd
import json
import logging
from typing import Tuple, Dict, List, Optional, Union
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import pickle
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GamePredictor:
    """
    Prediction head that uses team embeddings to predict game outcomes.
    """
    
    def __init__(self, model_type: str = 'knn'):
        self.model_type = model_type
        self.scaler = StandardScaler()
        
        # Initialize models based on type
        if model_type == 'knn':
            self.spread_model = KNeighborsRegressor(n_neighbors=15, weights='distance')
            self.total_model = KNeighborsRegressor(n_neighbors=15, weights='distance')
        elif model_type == 'rf':
            self.spread_model = RandomForestRegressor(n_estimators=100, random_state=42)
            self.total_model = RandomForestRegressor(n_estimators=100, random_state=42)
        else:  # ridge
            self.spread_model = Ridge(alpha=1.0)
            self.total_model = Ridge(alpha=1.0)
        
        self.is_trained = False
        self.team_embeddings = {}
        self.team_mapping = {}
        
    def load_team_embeddings(self, embeddings_path: str):
        """Load pre-trained team embeddings."""
        logger.info(f"Loading team embeddings from {embeddings_path}")
        
        with open(embeddings_path, 'r') as f:
            embedding_data = json.load(f)
        
        teams = embedding_data['teams']
        embeddings = np.array(embedding_data['embeddings'])
        
        # Create mapping from team name to embedding
        for i, team_info in enumerate(teams):
            team_name = team_info['team']
            self.team_embeddings[team_name] = embeddings[i]
            self.team_mapping[team_name] = i
        
        logger.info(f"Loaded embeddings for {len(self.team_embeddings)} teams")
    
    def create_matchup_features(self, team1: str, team2: str, home_advantage: float = 2.5, neutral_site: bool = False) -> np.ndarray:
        """Create features for a specific matchup."""
        
        # Handle missing teams with fallback embeddings
        def get_team_embedding(team_name):
            if team_name in self.team_embeddings:
                return self.team_embeddings[team_name]
            else:
                # Create fallback embedding for missing teams
                # Use average of similar conference/division teams or generic fallback
                logger.warning(f"Missing embedding for {team_name}, using fallback")
                
                # For FCS teams, use a weaker baseline
                if team_name in ['Western Illinois', 'Eastern Washington']:
                    # Create a "weak FCS team" embedding (below average in all metrics)
                    fallback_embedding = np.random.normal(-0.5, 0.3, 32)  # Below average
                else:
                    # For other missing teams, use neutral embedding
                    fallback_embedding = np.random.normal(0, 0.5, 32)  # Average
                
                return fallback_embedding
        
        # Check if teams are missing and warn user
        missing = []
        if team1 not in self.team_embeddings:
            missing.append(team1)
        if team2 not in self.team_embeddings:
            missing.append(team2)
        
        if missing:
            logger.warning(f"Using fallback embeddings for: {missing}. Predictions may be less accurate.")
        
        # Get team embeddings (with fallback handling)
        team1_emb = get_team_embedding(team1)
        team2_emb = get_team_embedding(team2)
        
        # Create feature vector
        features = []
        
        # 1. Raw embeddings
        features.extend(team1_emb)
        features.extend(team2_emb)
        
        # 2. Embedding differences
        embedding_diff = team1_emb - team2_emb
        features.extend(embedding_diff)
        
        # 3. Embedding interactions
        dot_product = np.dot(team1_emb, team2_emb)
        euclidean_dist = np.linalg.norm(team1_emb - team2_emb)
        cosine_sim = dot_product / (np.linalg.norm(team1_emb) * np.linalg.norm(team2_emb))
        
        features.extend([dot_product, euclidean_dist, cosine_sim])
        
        # 4. Home field advantage
        if neutral_site:
            home_field = 0.0
        else:
            home_field = home_advantage
        
        features.append(home_field)
        
        return np.array(features)
    
    def prepare_training_data(self, games_data: List[Dict], home_advantage: float = 2.5) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Prepare training data from historical games."""
        
        X = []
        y_spread = []  # Home team spread
        y_total = []   # Total points
        
        for game in games_data:
            try:
                home_team = game['home_team']
                away_team = game['away_team'] 
                home_score = float(game['home_score'])
                away_score = float(game['away_score'])
                neutral_site = game.get('neutral_site', False)
                
                # Skip games with missing scores
                if pd.isna(home_score) or pd.isna(away_score):
                    continue
                
                # Create features
                features = self.create_matchup_features(home_team, away_team, home_advantage, neutral_site)
                
                # Calculate targets
                spread = home_score - away_score  # Positive = home team wins
                total = home_score + away_score
                
                X.append(features)
                y_spread.append(spread)
                y_total.append(total)
                
            except (ValueError, KeyError) as e:
                continue
        
        logger.info(f"Prepared training data: {len(X)} games")
        
        return np.array(X), np.array(y_spread), np.array(y_total)
    
    def train(self, X: np.ndarray, y_spread: np.ndarray, y_total: np.ndarray):
        """Train the prediction models."""
        logger.info("Training prediction models...")
        
        # Normalize features
        X_scaled = self.scaler.fit_transform(X)
        
        # Train models
        self.spread_model.fit(X_scaled, y_spread)
        self.total_model.fit(X_scaled, y_total)
        
        self.is_trained = True
        
        # Calculate training metrics
        spread_pred = self.spread_model.predict(X_scaled)
        total_pred = self.total_model.predict(X_scaled)
        
        spread_mae = mean_absolute_error(y_spread, spread_pred)
        total_mae = mean_absolute_error(y_total, total_pred)
        spread_r2 = r2_score(y_spread, spread_pred)
        total_r2 = r2_score(y_total, total_pred)
        
        logger.info(f"Training Results:")
        logger.info(f"  Spread MAE: {spread_mae:.2f}, R2: {spread_r2:.3f}")
        logger.info(f"  Total MAE: {total_mae:.2f}, R2: {total_r2:.3f}")
        
        return {
            'spread_mae': spread_mae,
            'total_mae': total_mae,
            'spread_r2': spread_r2,
            'total_r2': total_r2
        }
    
    def predict_game(self, home_team: str, away_team: str, home_advantage: float = 2.5, neutral_site: bool = False) -> Dict[str, float]:
        """Predict the outcome of a single game."""
        
        if not self.is_trained:
            raise ValueError("Model must be trained before making predictions")
        
        # Create features
        features = self.create_matchup_features(home_team, away_team, home_advantage, neutral_site)
        features_scaled = self.scaler.transform(features.reshape(1, -1))
        
        # Make predictions
        spread_pred = self.spread_model.predict(features_scaled)[0]
        total_pred = self.total_model.predict(features_scaled)[0]
        
        # Calculate win probability
        spread_std = 10.0  
        home_win_prob = 1 / (1 + np.exp(-spread_pred / spread_std))
        
        return {
            'spread': round(spread_pred, 1),
            'total': round(total_pred, 1),
            'home_win_prob': round(home_win_prob, 3),
            'home_team': home_team,
            'away_team': away_team
        }
    
    def predict_multiple_games(self, games: List[Dict]) -> List[Dict]:
        """Predict outcomes for multiple games."""
        predictions = []
        
        for game in games:
            try:
                pred = self.predict_game(
                    game['home_team'],
                    game['away_team'],
                    neutral_site=game.get('neutral_site', False)
                )
                predictions.append(pred)
            except Exception as e:
                logger.warning(f"Failed to predict {game}: {e}")
                continue
        
        return predictions
    
    def save_model(self, filepath: str):
        """Save the trained prediction model."""
        model_data = {
            'model_type': self.model_type,
            'spread_model': self.spread_model,
            'total_model': self.total_model,
            'scaler': self.scaler,
            'is_trained': self.is_trained,
            'team_embeddings': self.team_embeddings,
            'team_mapping': self.team_mapping
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        logger.info(f"Prediction model saved to {filepath}")
    
    def load_model(self, filepath: str):
        """Load a trained prediction model."""
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model_type = model_data['model_type']
        self.spread_model = model_data['spread_model']
        self.total_model = model_data['total_model']
        self.scaler = model_data['scaler']
        self.is_trained = model_data['is_trained']
        self.team_embeddings = model_data['team_embeddings']
        self.team_mapping = model_data['team_mapping']
        
        logger.info(f"Prediction model loaded from {filepath}")

def load_historical_games(schedule_files: List[str]) -> List[Dict]:
    """Load historical game data for training."""
    import sys
    sys.path.append('src')
    from utils.team_name_mapping import normalize_team_name, create_team_name_mapping
    
    mapping = create_team_name_mapping()
    all_games = []
    
    for file_path in schedule_files:
        try:
            with open(file_path, 'r') as f:
                games = json.load(f)
            
            for game in games:
                if game.get('Status') == 'Final':
                    # Normalize team names to match embeddings
                    home_team_full = game.get('HomeTeamName')
                    away_team_full = game.get('AwayTeamName')
                    
                    home_team_short = normalize_team_name(home_team_full, mapping)
                    away_team_short = normalize_team_name(away_team_full, mapping)
                    
                    game_data = {
                        'home_team': home_team_short,
                        'away_team': away_team_short,
                        'home_score': game.get('HomeTeamScore'),
                        'away_score': game.get('AwayTeamScore'),
                        'neutral_site': game.get('NeutralSite', False)
                    }
                    
                    # Only add games with valid scores
                    if (game_data['home_score'] is not None and 
                        game_data['away_score'] is not None):
                        all_games.append(game_data)
                        
            logger.info(f"Loaded {len([g for g in games if g.get('Status') == 'Final'])} completed games from {file_path}")
            
        except Exception as e:
            logger.warning(f"Failed to load {file_path}: {e}")
    
    logger.info(f"Total historical games loaded: {len(all_games)}")
    return all_games

def main():
    """Main training script for prediction head."""
    print("Step 4: Prediction Head Model")
    print("=" * 30)
    
    try:
        # Initialize predictor
        predictor = GamePredictor(model_type='knn')
        
        # Load team embeddings
        print("Loading team embeddings...")
        predictor.load_team_embeddings('data/models/team_embeddings_embeddings.json')
        
        # Load historical game data
        print("Loading historical games for training...")
        schedule_files = ['data/raw/schedule_2024.json']
        
        historical_games = load_historical_games(schedule_files)
        
        if len(historical_games) < 100:
            print(f"Warning: Only {len(historical_games)} games found for training")
        
        # Prepare training data
        print("Preparing training data...")
        X, y_spread, y_total = predictor.prepare_training_data(historical_games)
        
        if len(X) == 0:
            print("Error: No valid training data found")
            return None
        
        # Train model
        print(f"Training on {len(X)} games...")
        metrics = predictor.train(X, y_spread, y_total)
        
        # Save trained model
        print("Saving trained model...")
        predictor.save_model('data/models/prediction_head.pkl')
        
        # Test with sample predictions
        print("\nSample Predictions:")
        sample_games = [
            {'home_team': 'Ohio State', 'away_team': 'Michigan'},
            {'home_team': 'Georgia', 'away_team': 'Alabama'},
            {'home_team': 'Oregon', 'away_team': 'Washington'}
        ]
        
        for game in sample_games:
            try:
                pred = predictor.predict_game(game['home_team'], game['away_team'])
                print(f"  {pred['home_team']} vs {pred['away_team']}")
                print(f"    Spread: {pred['home_team']} -{pred['spread']}")
                print(f"    Total: {pred['total']}")
                print(f"    {pred['home_team']} Win Prob: {pred['home_win_prob']*100:.1f}%")
                print()
            except Exception as e:
                print(f"    Could not predict {game}: {e}")
        
        print("Step 4 Complete! Ready for Step 5: Monte Carlo Simulation!")
        
        return predictor, metrics
        
    except Exception as e:
        print(f"Error in prediction head training: {e}")
        logger.error(f"Prediction head error: {e}")
        return None

if __name__ == "__main__":
    main()