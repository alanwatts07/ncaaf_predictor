# config.py
import os
from typing import Dict

def get_config() -> Dict:
    """
    Get configuration settings for the NCAA data collector.
    
    Returns:
        Dict: Configuration dictionary with API keys and settings
    """
    config = {
        # API Keys (you'll need to get these from the respective services)
        'cfbd_api_key': os.getenv('CFBD_API_KEY', '8O0ClYn1Aw1thycgcxSKbemsHWvNSZMYhoyEkR4KZAHwrggwBYbDv6gXPKqbFcZs'),
        'odds_api_key': os.getenv('ODDS_API_KEY', 'your_odds_api_key_here'),
        
        # Redis Configuration
        'redis_host': os.getenv('REDIS_HOST', 'localhost'),
        'redis_port': int(os.getenv('REDIS_PORT', 6379)),
        
        # Data Collection Settings
        'current_season': 2024,
        'seasons_to_collect': [2020, 2021, 2022, 2023, 2024],
        
        # Output Settings
        'output_dir': 'data/raw/',
        'save_to_files': True,
        'file_format': 'json'  # or 'csv'
    }
    
    return config

# API Key Setup Instructions
API_SETUP_INSTRUCTIONS = """
To get your API keys:

1. CFBD API Key (FREE):
   - Go to https://collegefootballdata.com/
   - Create an account
   - Generate your API key
   - Set environment variable: export CFBD_API_KEY=your_key_here

2. The Odds API Key (FREE tier available):
   - Go to https://the-odds-api.com/
   - Sign up for an account
   - Get your API key from the dashboard
   - Set environment variable: export ODDS_API_KEY=your_key_here

3. Redis Setup (for caching):
   - Install Redis: brew install redis (Mac) or apt-get install redis (Ubuntu)
   - Start Redis: redis-server
   - Or use Docker: docker run -d -p 6379:6379 redis:alpine
"""