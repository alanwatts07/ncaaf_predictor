# src/data_pipeline/preprocessing.py
import pandas as pd
import numpy as np
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class NCAADataPreprocessor:
    """
    Preprocessor for NCAA football data to clean, normalize, and prepare data for database storage.
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.input_dir = Path(config['input_dir'])
        self.output_dir = Path(config['output_dir'])
        self.db_path = config.get('db_path', 'data/ncaa_football.db')
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(f'sqlite:///{self.db_path}')
        
    def load_json_data(self, filename: str) -> List[Dict]:
        """Load data from a JSON file."""
        file_path = self.input_dir / f"{filename}.json"
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return []
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} records from {filename}.json")
            return data
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"Error loading {filename}.json: {e}")
            return []

    def create_database_tables(self):
        """Create database tables with a normalized schema for SportsData.IO."""
        logger.info("Creating database tables...")
    
        with self.engine.connect() as conn:
            # Teams table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS teams (
                    TeamID INTEGER PRIMARY KEY,
                    Key TEXT,
                    School TEXT,
                    Name TEXT,
                    ConferenceID INTEGER,
                    Conference TEXT,
                    TeamLogoUrl TEXT
                )
            """))
    
            # Games table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS games (
                    GameID INTEGER PRIMARY KEY,
                    Season INTEGER,
                    Week INTEGER,
                    Day TEXT,
                    DateTime TEXT,
                    Status TEXT,
                    HomeTeamID INTEGER,
                    AwayTeamID INTEGER,
                    HomeTeamName TEXT,
                    AwayTeamName TEXT,
                    HomeTeamScore INTEGER,
                    AwayTeamScore INTEGER,
                    FOREIGN KEY (HomeTeamID) REFERENCES teams (TeamID),
                    FOREIGN KEY (AwayTeamID) REFERENCES teams (TeamID)
                )
            """))
    
            # Player Game Stats table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS player_game_stats (
                    PlayerGameID INTEGER PRIMARY KEY AUTOINCREMENT,
                    PlayerID INTEGER,
                    GameID INTEGER,
                    TeamID INTEGER,
                    Name TEXT,
                    Position TEXT,
                    FantasyPoints REAL,
                    PassingAttempts REAL,
                    PassingCompletions REAL,
                    PassingYards REAL,
                    PassingTouchdowns REAL,
                    PassingInterceptions REAL,
                    RushingAttempts REAL,
                    RushingYards REAL,
                    RushingTouchdowns REAL,
                    ReceivingTargets REAL,
                    Receptions REAL,
                    ReceivingYards REAL,
                    ReceivingTouchdowns REAL,
                    FOREIGN KEY (GameID) REFERENCES games (GameID),
                    FOREIGN KEY (TeamID) REFERENCES teams (TeamID)
                )
            """))
            
            # Team Game Stats table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS team_game_stats (
                    TeamGameID INTEGER PRIMARY KEY AUTOINCREMENT,
                    GameID INTEGER,
                    TeamID INTEGER,
                    Score INTEGER,
                    PassingYards REAL,
                    RushingYards REAL,
                    Turnovers REAL,
                    FirstDowns REAL,
                    FOREIGN KEY (GameID) REFERENCES games (GameID),
                    FOREIGN KEY (TeamID) REFERENCES teams (TeamID)
                )
            """))
    
            # Betting Events and Markets
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS betting_events (
                    BettingEventID INTEGER PRIMARY KEY,
                    GameID INTEGER,
                    Name TEXT,
                    FOREIGN KEY (GameID) REFERENCES games (GameID)
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS betting_markets (
                    BettingMarketID INTEGER PRIMARY KEY,
                    BettingEventID INTEGER,
                    BettingMarketType TEXT,
                    PlayerID INTEGER,
                    Sportsbook TEXT,
                    Name TEXT,
                    Value REAL,
                    FOREIGN KEY (BettingEventID) REFERENCES betting_events (BettingEventID)
                )
            """))
            
            # SP+ Ratings table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sp_plus_ratings (
                    RatingID INTEGER PRIMARY KEY AUTOINCREMENT,
                    Year INTEGER,
                    Team TEXT,
                    Conference TEXT,
                    Rating REAL,
                    SecondOrderWins REAL,
                    SOS REAL,
                    OffenseRating REAL,
                    DefenseRating REAL,
                    SpecialTeamsRating REAL
                )
            """))
            
            # Opponent Adjusted Stats table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS opponent_adjusted_stats (
                    AdjStatID INTEGER PRIMARY KEY AUTOINCREMENT,
                    Team TEXT,
                    Conference TEXT,
                    Year INTEGER,
                    OffTotalPPA REAL,
                    OffSuccessRate REAL,
                    OffExplosiveness REAL,
                    OffRushingPPA REAL,
                    OffPassingPPA REAL,
                    OffStandardDownsPPA REAL,
                    OffPassingDownsPPA REAL,
                    DefTotalPPA REAL,
                    DefSuccessRate REAL,
                    DefExplosiveness REAL,
                    DefRushingPPA REAL,
                    DefPassingPPA REAL,
                    DefStandardDownsPPA REAL,
                    DefPassingDownsPPA REAL
                )
            """))
            
            # FPI Ratings table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fpi_ratings (
                    FPIID INTEGER PRIMARY KEY AUTOINCREMENT,
                    Year INTEGER,
                    Team TEXT,
                    Conference TEXT,
                    FPI REAL,
                    FPIRank INTEGER,
                    StrengthOfRecord REAL,
                    FPIOffense REAL,
                    FPIDefense REAL,
                    EfficiencyOffense REAL,
                    EfficiencyDefense REAL
                )
            """))
            
            # Team Talent Rankings table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS talent_rankings (
                    TalentID INTEGER PRIMARY KEY AUTOINCREMENT,
                    Year INTEGER,
                    School TEXT,
                    Talent REAL,
                    TalentRank INTEGER
                )
            """))
            
            # Recruiting Rankings table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS recruiting_rankings (
                    RecruitingID INTEGER PRIMARY KEY AUTOINCREMENT,
                    Year INTEGER,
                    Team TEXT,
                    Rank INTEGER,
                    Points REAL
                )
            """))
            
            # PPA Team Stats table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ppa_team_stats (
                    PPAID INTEGER PRIMARY KEY AUTOINCREMENT,
                    Season INTEGER,
                    Team TEXT,
                    Conference TEXT,
                    OffenseOverall REAL,
                    OffensePassing REAL,
                    OffenseRushing REAL,
                    OffenseFirstDown REAL,
                    OffenseSecondDown REAL,
                    OffenseThirdDown REAL,
                    DefenseOverall REAL,
                    DefensePassing REAL,
                    DefenseRushing REAL,
                    DefenseFirstDown REAL,
                    DefenseSecondDown REAL,
                    DefenseThirdDown REAL
                )
            """))
            
            conn.commit()
    
        logger.info("Database tables created successfully")

    def save_to_database(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
        """Save DataFrame to database."""
        if df.empty:
            logger.warning(f"Empty DataFrame for table {table_name}, skipping save.")
            return
        
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            logger.info(f"Saved {len(df)} records to '{table_name}' table")
        except Exception as e:
            logger.error(f"Error saving to {table_name}: {e}")

    def preprocess_teams_data(self) -> pd.DataFrame:
        """Preprocess teams data."""
        logger.info("Preprocessing teams data...")
        teams_data = self.load_json_data("teams")
        if not teams_data:
            return pd.DataFrame()

        df = pd.DataFrame(teams_data)
        df = df[['TeamID', 'Key', 'School', 'Name', 'ConferenceID', 'Conference', 'TeamLogoUrl']]
        return df

    def preprocess_schedule_data(self, season: int) -> pd.DataFrame:
        """Preprocess schedule/games data."""
        logger.info(f"Preprocessing schedule data for {season}...")
        schedule_data = self.load_json_data(f"schedule_{season}")
        if not schedule_data:
            return pd.DataFrame()

        df = pd.DataFrame(schedule_data)
        df = df[['GameID', 'Season', 'Week', 'Day', 'DateTime', 'Status', 'HomeTeamID', 'AwayTeamID', 'HomeTeamName', 'AwayTeamName', 'HomeTeamScore', 'AwayTeamScore']]
        return df

    def preprocess_player_game_stats(self, season: int, week: int) -> pd.DataFrame:
        """Preprocess player game statistics."""
        logger.info(f"Preprocessing player game stats for {season}, week {week}...")
        player_game_data = self.load_json_data(f"player_game_stats_by_week_{season}_{week}")
        if not player_game_data:
            return pd.DataFrame()

        df = pd.DataFrame(player_game_data)
        df = df[['PlayerID', 'GameID', 'TeamID', 'Name', 'Position', 'FantasyPoints', 'PassingAttempts', 'PassingCompletions', 'PassingYards', 'PassingTouchdowns', 'PassingInterceptions', 'RushingAttempts', 'RushingYards', 'RushingTouchdowns', 'ReceivingTargets', 'Receptions', 'ReceivingYards', 'ReceivingTouchdowns']]
        return df
        
    def preprocess_team_game_stats(self, season: int, week: int) -> pd.DataFrame:
        """Preprocess team game statistics."""
        logger.info(f"Preprocessing team game stats for {season}, week {week}...")
        team_game_data = self.load_json_data(f"team_game_stats_by_week_{season}_{week}")
        if not team_game_data:
            return pd.DataFrame()

        df = pd.DataFrame(team_game_data)
        df = df[['GameID', 'TeamID', 'Score', 'PassingYards', 'RushingYards', 'Turnovers', 'FirstDowns']]
        return df
    
    def process_season_data(self, season: int):
        """Process all data for a given season."""
        logger.info(f"Processing all data for season {season}")

        # Always ensure tables exist
        self.create_database_tables()

        # Process teams (usually once per season is enough, but good to ensure it's up-to-date)
        teams_df = self.preprocess_teams_data()
        self.save_to_database(teams_df, 'teams', if_exists='replace') # Use replace for teams to keep it fresh

        # Process schedule
        games_df = self.preprocess_schedule_data(season)
        self.save_to_database(games_df, 'games')

        # Process weekly stats for the entire season
        # Assuming a season has 15 weeks (regular + bowls)
        for week in range(1, 16):
            logger.info(f"--- Processing week {week} for {season} ---")
            
            # Player Game Stats
            player_stats_df = self.preprocess_player_game_stats(season, week)
            self.save_to_database(player_stats_df, 'player_game_stats')
            
            # Team Game Stats
            team_stats_df = self.preprocess_team_game_stats(season, week)
            self.save_to_database(team_stats_df, 'team_game_stats')

        logger.info(f"Completed processing for season {season}")

    def preprocess_sp_plus_ratings(self, year: int) -> pd.DataFrame:
        """Preprocess SP+ ratings data."""
        logger.info(f"Preprocessing SP+ ratings for {year}...")
        sp_data = self.load_json_data(f"sp_plus_ratings_{year}")
        if not sp_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(sp_data)
        
        # Handle different possible field names from API
        columns_mapping = {
            'year': 'Year',
            'team': 'Team', 
            'conference': 'Conference',
            'rating': 'Rating',
            'secondOrderWins': 'SecondOrderWins',
            'sos': 'SOS',
            'offense': 'OffenseRating',
            'defense': 'DefenseRating',
            'specialTeams': 'SpecialTeamsRating'
        }
        
        # Rename columns if they exist
        for old_col, new_col in columns_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
        
        # Ensure Year column
        if 'Year' not in df.columns:
            df['Year'] = year
            
        return df

    def preprocess_opponent_adjusted_stats(self, year: int) -> pd.DataFrame:
        """Preprocess opponent-adjusted statistics."""
        logger.info(f"Preprocessing opponent-adjusted stats for {year}...")
        adj_data = self.load_json_data(f"opponent_adjusted_stats_{year}")
        if not adj_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(adj_data)
        
        # Map column names to database schema
        columns_mapping = {
            'team': 'Team',
            'conference': 'Conference', 
            'year': 'Year',
            'off_total_ppa': 'OffTotalPPA',
            'off_success_rate': 'OffSuccessRate',
            'off_explosiveness': 'OffExplosiveness',
            'off_rushing_ppa': 'OffRushingPPA',
            'off_passing_ppa': 'OffPassingPPA',
            'off_standard_downs_ppa': 'OffStandardDownsPPA',
            'off_passing_downs_ppa': 'OffPassingDownsPPA',
            'def_total_ppa': 'DefTotalPPA',
            'def_success_rate': 'DefSuccessRate',
            'def_explosiveness': 'DefExplosiveness',
            'def_rushing_ppa': 'DefRushingPPA',
            'def_passing_ppa': 'DefPassingPPA',
            'def_standard_downs_ppa': 'DefStandardDownsPPA',
            'def_passing_downs_ppa': 'DefPassingDownsPPA'
        }
        
        for old_col, new_col in columns_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
        
        return df

    def preprocess_fpi_ratings(self, year: int) -> pd.DataFrame:
        """Preprocess FPI ratings data."""
        logger.info(f"Preprocessing FPI ratings for {year}...")
        fpi_data = self.load_json_data(f"fpi_ratings_{year}")
        if not fpi_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(fpi_data)
        
        columns_mapping = {
            'year': 'Year',
            'team': 'Team',
            'conference': 'Conference',
            'fpi': 'FPI',
            'fpiRank': 'FPIRank',
            'strengthOfRecord': 'StrengthOfRecord',
            'fpiOffense': 'FPIOffense',
            'fpiDefense': 'FPIDefense',
            'efficiencyOffense': 'EfficiencyOffense',
            'efficiencyDefense': 'EfficiencyDefense'
        }
        
        for old_col, new_col in columns_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
        
        if 'Year' not in df.columns:
            df['Year'] = year
            
        return df

    def preprocess_talent_rankings(self, year: int) -> pd.DataFrame:
        """Preprocess team talent rankings."""
        logger.info(f"Preprocessing talent rankings for {year}...")
        talent_data = self.load_json_data(f"talent_rankings_{year}")
        if not talent_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(talent_data)
        
        columns_mapping = {
            'year': 'Year',
            'school': 'School',
            'talent': 'Talent',
            'rank': 'TalentRank'
        }
        
        for old_col, new_col in columns_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
        
        if 'Year' not in df.columns:
            df['Year'] = year
            
        return df

    def preprocess_recruiting_rankings(self, year: int) -> pd.DataFrame:
        """Preprocess recruiting rankings."""
        logger.info(f"Preprocessing recruiting rankings for {year}...")
        recruiting_data = self.load_json_data(f"recruiting_rankings_{year}")
        if not recruiting_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(recruiting_data)
        
        columns_mapping = {
            'year': 'Year',
            'team': 'Team',
            'rank': 'Rank',
            'points': 'Points'
        }
        
        for old_col, new_col in columns_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
        
        if 'Year' not in df.columns:
            df['Year'] = year
            
        return df

    def preprocess_ppa_team_stats(self, year: int) -> pd.DataFrame:
        """Preprocess PPA team statistics."""
        logger.info(f"Preprocessing PPA team stats for {year}...")
        ppa_data = self.load_json_data(f"ppa_team_stats_{year}")
        if not ppa_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(ppa_data)
        
        # Handle nested structure from PPA API
        processed_data = []
        for row in ppa_data:
            processed_row = {
                'Season': row.get('season', year),
                'Team': row.get('team'),
                'Conference': row.get('conference'),
            }
            
            # Extract offense stats
            offense = row.get('offense', {})
            processed_row.update({
                'OffenseOverall': offense.get('overall'),
                'OffensePassing': offense.get('passing'),
                'OffenseRushing': offense.get('rushing'),
                'OffenseFirstDown': offense.get('firstDown'),
                'OffenseSecondDown': offense.get('secondDown'),
                'OffenseThirdDown': offense.get('thirdDown')
            })
            
            # Extract defense stats
            defense = row.get('defense', {})
            processed_row.update({
                'DefenseOverall': defense.get('overall'),
                'DefensePassing': defense.get('passing'),
                'DefenseRushing': defense.get('rushing'),
                'DefenseFirstDown': defense.get('firstDown'),
                'DefenseSecondDown': defense.get('secondDown'),
                'DefenseThirdDown': defense.get('thirdDown')
            })
            
            processed_data.append(processed_row)
        
        return pd.DataFrame(processed_data)

    def process_advanced_season_data(self, year: int):
        """Process all advanced data for a given season."""
        logger.info(f"Processing advanced data for season {year}")

        # Always ensure tables exist
        self.create_database_tables()

        # Process SP+ ratings (most important)
        sp_plus_df = self.preprocess_sp_plus_ratings(year)
        self.save_to_database(sp_plus_df, 'sp_plus_ratings')

        # Process opponent-adjusted stats
        adj_stats_df = self.preprocess_opponent_adjusted_stats(year)
        self.save_to_database(adj_stats_df, 'opponent_adjusted_stats')

        # Process FPI ratings
        fpi_df = self.preprocess_fpi_ratings(year)
        self.save_to_database(fpi_df, 'fpi_ratings')

        # Process talent rankings
        talent_df = self.preprocess_talent_rankings(year)
        self.save_to_database(talent_df, 'talent_rankings')

        # Process recruiting rankings
        recruiting_df = self.preprocess_recruiting_rankings(year)
        self.save_to_database(recruiting_df, 'recruiting_rankings')

        # Process PPA team stats
        ppa_df = self.preprocess_ppa_team_stats(year)
        self.save_to_database(ppa_df, 'ppa_team_stats')

        logger.info(f"Completed processing advanced data for season {year}")

    def get_summary_stats(self) -> Dict:
        """Get summary statistics of the processed data."""
        summary = {}
        with self.engine.connect() as conn:
            tables = ['teams', 'games', 'player_game_stats', 'team_game_stats', 'betting_events', 'betting_markets', 
                     'sp_plus_ratings', 'opponent_adjusted_stats', 'fpi_ratings', 'talent_rankings', 
                     'recruiting_rankings', 'ppa_team_stats']
            for table in tables:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    summary[f'{table}_count'] = count
                except Exception as e:
                    summary[f'{table}_count'] = f"Error: {e}"
        return summary