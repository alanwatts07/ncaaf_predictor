# src/features/build_features.py
import pandas as pd
import numpy as np
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
import warnings

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FootballFeatureEngineer:
    """
    Feature engineering pipeline for NCAA football prediction models.
    
    Converts raw opponent-adjusted metrics into ML-ready features for
    team embeddings and game predictions.
    """
    
    def __init__(self, data_dir: str = "data/raw/"):
        self.data_dir = Path(data_dir)
        self.scaler = StandardScaler()
        self.team_encoder = LabelEncoder()
        self.features_cache = {}
        
    def load_advanced_data(self, year: int = 2024) -> Dict[str, pd.DataFrame]:
        """Load all advanced metrics data for a given year."""
        logger.info(f"Loading advanced data for {year}...")
        
        datasets = {}
        
        # Key datasets for feature engineering
        data_files = {
            'sp_plus': f'sp_plus_ratings_{year}.json',
            'opponent_adj': f'opponent_adjusted_stats_{year}.json', 
            'advanced_stats': f'advanced_team_stats_{year}.json',
            'ppa_stats': f'ppa_team_stats_{year}.json',
            'fpi': f'fpi_ratings_{year}.json',
            'talent': f'talent_rankings_{year}.json',
            'recruiting': f'recruiting_rankings_{year}.json'
        }
        
        for key, filename in data_files.items():
            file_path = self.data_dir / filename
            if file_path.exists():
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    datasets[key] = pd.DataFrame(data)
                    logger.info(f" Loaded {key}: {len(data)} records")
                except Exception as e:
                    logger.warning(f"L Failed to load {key}: {e}")
                    datasets[key] = pd.DataFrame()
            else:
                logger.warning(f"L File not found: {filename}")
                datasets[key] = pd.DataFrame()
        
        return datasets
    
    def create_team_feature_matrix(self, datasets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Create comprehensive team feature matrix from all advanced metrics.
        
        This is the core feature matrix that will be used for team embeddings.
        """
        logger.info("Creating team feature matrix...")
        
        # Start with SP+ ratings (most important)
        if not datasets['sp_plus'].empty:
            team_features = datasets['sp_plus'][['team', 'conference', 'rating']].copy()
            team_features = team_features.rename(columns={'rating': 'sp_plus_rating'})
            
            # Add SP+ offense/defense components
            sp_plus_expanded = datasets['sp_plus'].copy()
            team_features['sp_plus_offense'] = sp_plus_expanded['offense'].apply(
                lambda x: x.get('rating', 0) if isinstance(x, dict) else 0
            )
            team_features['sp_plus_defense'] = sp_plus_expanded['defense'].apply(
                lambda x: x.get('rating', 0) if isinstance(x, dict) else 0
            )
            team_features['sp_plus_special_teams'] = sp_plus_expanded['specialTeams'].apply(
                lambda x: x.get('rating', 0) if isinstance(x, dict) else 0
            )
        else:
            logger.warning("No SP+ data available")
            return pd.DataFrame()
        
        # Merge opponent-adjusted stats
        if not datasets['opponent_adj'].empty:
            opp_adj = datasets['opponent_adj'].copy()
            
            # Key opponent-adjusted metrics
            opp_features = opp_adj[[
                'team', 'off_total_ppa', 'off_success_rate', 'off_explosiveness',
                'def_total_ppa', 'def_success_rate', 'def_explosiveness'
            ]].copy()
            
            team_features = team_features.merge(opp_features, on='team', how='left')
        
        # Merge FPI ratings
        if not datasets['fpi'].empty:
            fpi_features = datasets['fpi'][['team', 'fpi']].copy()
            team_features = team_features.merge(fpi_features, on='team', how='left')
        
        # Merge PPA team stats
        if not datasets['ppa_stats'].empty:
            ppa_stats = datasets['ppa_stats'].copy()
            
            # Extract nested PPA metrics
            ppa_features = pd.DataFrame()
            ppa_features['team'] = ppa_stats['team']
            
            # Offensive PPA metrics
            for _, row in ppa_stats.iterrows():
                team_name = row['team']
                offense = row.get('offense', {}) if isinstance(row.get('offense'), dict) else {}
                defense = row.get('defense', {}) if isinstance(row.get('defense'), dict) else {}
                
                ppa_row = {
                    'team': team_name,
                    'ppa_off_overall': offense.get('overall', 0),
                    'ppa_off_passing': offense.get('passing', 0),
                    'ppa_off_rushing': offense.get('rushing', 0),
                    'ppa_def_overall': defense.get('overall', 0),
                    'ppa_def_passing': defense.get('passing', 0),
                    'ppa_def_rushing': defense.get('rushing', 0)
                }
                ppa_features = pd.concat([ppa_features, pd.DataFrame([ppa_row])], ignore_index=True)
            
            team_features = team_features.merge(ppa_features, on='team', how='left')
        
        # Merge talent rankings
        if not datasets['talent'].empty:
            talent_features = datasets['talent'][['team', 'talent']].copy()
            team_features = team_features.merge(talent_features, on='team', how='left')
        
        # Merge recruiting rankings
        if not datasets['recruiting'].empty:
            recruiting_features = datasets['recruiting'][['team', 'points']].copy()
            recruiting_features = recruiting_features.rename(columns={'points': 'recruiting_points'})
            team_features = team_features.merge(recruiting_features, on='team', how='left')
        
        # Add derived features
        team_features = self._add_derived_features(team_features)
        
        # Handle missing values
        team_features = self._handle_missing_values(team_features)
        
        logger.info(f" Created feature matrix: {team_features.shape[0]} teams, {team_features.shape[1]} features")
        
        return team_features
    
    def _add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived features from raw metrics."""
        df = df.copy()
        
        # Offensive vs Defensive efficiency balance
        if 'sp_plus_offense' in df.columns and 'sp_plus_defense' in df.columns:
            df['sp_plus_balance'] = df['sp_plus_offense'] - df['sp_plus_defense']
        
        # Total team efficiency (offense - defense allowed)
        if 'off_total_ppa' in df.columns and 'def_total_ppa' in df.columns:
            df['net_ppa'] = df['off_total_ppa'] - df['def_total_ppa']
        
        # Success rate differential
        if 'off_success_rate' in df.columns and 'def_success_rate' in df.columns:
            df['success_rate_diff'] = df['off_success_rate'] - df['def_success_rate']
        
        # Explosiveness differential
        if 'off_explosiveness' in df.columns and 'def_explosiveness' in df.columns:
            df['explosiveness_diff'] = df['off_explosiveness'] - df['def_explosiveness']
        
        # PPA efficiency ratios
        if 'ppa_off_overall' in df.columns and 'ppa_def_overall' in df.columns:
            df['ppa_efficiency_ratio'] = df['ppa_off_overall'] / (df['ppa_def_overall'] + 0.001)  # Avoid division by zero
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in feature matrix."""
        df = df.copy()
        
        # Fill numeric columns with median
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if col not in ['team']:  # Don't fill team names
                df[col] = df[col].fillna(df[col].median())
        
        # Fill any remaining NaN with 0
        df = df.fillna(0)
        
        return df
    
    def prepare_embedding_features(self, team_features: pd.DataFrame) -> Tuple[np.ndarray, List[str], pd.DataFrame]:
        """
        Prepare features specifically for the VME (team embedding) model.
        
        Returns normalized feature matrix, feature names, and team info.
        """
        logger.info("Preparing embedding features...")
        
        # Define key features for embeddings (most predictive metrics)
        embedding_feature_columns = [
            'sp_plus_rating', 'sp_plus_offense', 'sp_plus_defense',
            'off_total_ppa', 'off_success_rate', 'off_explosiveness',
            'def_total_ppa', 'def_success_rate', 'def_explosiveness', 
            'fpi', 'talent', 'recruiting_points',
            'net_ppa', 'success_rate_diff', 'explosiveness_diff'
        ]
        
        # Select available features
        available_features = [col for col in embedding_feature_columns if col in team_features.columns]
        logger.info(f"Using {len(available_features)} features for embeddings: {available_features}")
        
        # Create feature matrix
        X = team_features[available_features].values
        
        # Normalize features
        X_normalized = self.scaler.fit_transform(X)
        
        # Team info for reference
        team_info = team_features[['team', 'conference']].copy()
        
        logger.info(f" Embedding features prepared: {X_normalized.shape}")
        
        return X_normalized, available_features, team_info
    
    def create_game_matchup_features(self, team1: str, team2: str, team_features: pd.DataFrame, 
                                   home_advantage: bool = True) -> np.ndarray:
        """
        Create features for a specific game matchup.
        
        This will be used by the prediction head model.
        """
        # Get team features
        team1_features = team_features[team_features['team'] == team1]
        team2_features = team_features[team_features['team'] == team2]
        
        if team1_features.empty or team2_features.empty:
            logger.warning(f"Missing team data for {team1} vs {team2}")
            return np.array([])
        
        # Key matchup features
        feature_columns = [
            'sp_plus_rating', 'sp_plus_offense', 'sp_plus_defense',
            'off_total_ppa', 'def_total_ppa', 'fpi', 'talent'
        ]
        
        available_cols = [col for col in feature_columns if col in team_features.columns]
        
        # Team 1 features
        t1_features = team1_features[available_cols].values.flatten()
        # Team 2 features  
        t2_features = team2_features[available_cols].values.flatten()
        
        # Differential features (Team1 - Team2)
        diff_features = t1_features - t2_features
        
        # Home field advantage
        home_advantage_feature = [1.0 if home_advantage else 0.0]
        
        # Combine all features
        matchup_features = np.concatenate([
            t1_features, t2_features, diff_features, home_advantage_feature
        ])
        
        return matchup_features
    
    def save_features(self, team_features: pd.DataFrame, filename: str = "team_features_2024.json"):
        """Save processed features for later use."""
        output_path = self.data_dir / filename
        team_features.to_json(output_path, indent=2)
        logger.info(f" Features saved to {output_path}")


def main():
    """Main feature engineering pipeline."""
    print("Step 2: Feature Engineering Pipeline")
    print("=" * 40)
    
    # Initialize feature engineer
    engineer = FootballFeatureEngineer()
    
    # Load 2024 data
    print("=� Loading 2024 advanced data...")
    datasets = engineer.load_advanced_data(2024)
    
    # Create team feature matrix
    print("=' Creating team feature matrix...")
    team_features = engineer.create_team_feature_matrix(datasets)
    
    if not team_features.empty:
        print(f" Feature matrix created: {team_features.shape}")
        print(f"=� Features: {list(team_features.columns)}")
        
        # Prepare embedding features
        print("<� Preparing embedding features...")
        X_normalized, feature_names, team_info = engineer.prepare_embedding_features(team_features)
        
        print(f" Embedding features ready: {X_normalized.shape}")
        print(f"=� Sample team: {team_info.iloc[0]['team']} -> {X_normalized[0][:5]}...")
        
        # Save features
        engineer.save_features(team_features)
        
        print("\n<� Ready for Step 3: Team Embedding Model!")
        
        return team_features, X_normalized, feature_names, team_info
    else:
        print("L Feature engineering failed")
        return None

if __name__ == "__main__":
    main()