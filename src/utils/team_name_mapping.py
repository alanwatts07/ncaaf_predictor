# src/utils/team_name_mapping.py
"""
Team name mapping utilities to handle different naming conventions
between data sources (schedule vs embeddings).
"""

def create_team_name_mapping():
    """
    Create mapping from full team names (with mascots) to short team names.
    """
    mapping = {
        # Major teams
        'Alabama Crimson Tide': 'Alabama',
        'Auburn Tigers': 'Auburn',
        'Georgia Bulldogs': 'Georgia',
        'Florida Gators': 'Florida',
        'LSU Tigers': 'LSU',
        'Tennessee Volunteers': 'Tennessee',
        'Kentucky Wildcats': 'Kentucky',
        'South Carolina Gamecocks': 'South Carolina',
        'Arkansas Razorbacks': 'Arkansas',
        'Mississippi State Bulldogs': 'Mississippi State',
        'Ole Miss Rebels': 'Ole Miss',
        'Missouri Tigers': 'Missouri',
        'Texas A&M Aggies': 'Texas A&M',
        'Vanderbilt Commodores': 'Vanderbilt',
        
        # Big Ten
        'Ohio State Buckeyes': 'Ohio State',
        'Michigan Wolverines': 'Michigan',
        'Penn State Nittany Lions': 'Penn State',
        'Michigan State Spartans': 'Michigan State',
        'Wisconsin Badgers': 'Wisconsin',
        'Iowa Hawkeyes': 'Iowa',
        'Minnesota Golden Gophers': 'Minnesota',
        'Illinois Fighting Illini': 'Illinois',
        'Northwestern Wildcats': 'Northwestern',
        'Indiana Hoosiers': 'Indiana',
        'Purdue Boilermakers': 'Purdue',
        'Maryland Terrapins': 'Maryland',
        'Rutgers Scarlet Knights': 'Rutgers',
        'Nebraska Cornhuskers': 'Nebraska',
        
        # Big 12
        'Texas Longhorns': 'Texas',
        'Oklahoma Sooners': 'Oklahoma',
        'Oklahoma State Cowboys': 'Oklahoma State',
        'Kansas Jayhawks': 'Kansas',
        'Kansas State Wildcats': 'Kansas State',
        'Iowa State Cyclones': 'Iowa State',
        'Texas Tech Red Raiders': 'Texas Tech',
        'Baylor Bears': 'Baylor',
        'TCU Horned Frogs': 'TCU',
        'West Virginia Mountaineers': 'West Virginia',
        
        # Pac-12
        'Oregon Ducks': 'Oregon',
        'Washington Huskies': 'Washington',
        'USC Trojans': 'USC',
        'UCLA Bruins': 'UCLA',
        'Stanford Cardinal': 'Stanford',
        'California Golden Bears': 'California',
        'Arizona Wildcats': 'Arizona',
        'Arizona State Sun Devils': 'Arizona State',
        'Utah Utes': 'Utah',
        'Colorado Buffaloes': 'Colorado',
        'Washington State Cougars': 'Washington State',
        'Oregon State Beavers': 'Oregon State',
        
        # ACC
        'Clemson Tigers': 'Clemson',
        'Florida State Seminoles': 'Florida State',
        'Miami Hurricanes': 'Miami',
        'North Carolina Tar Heels': 'North Carolina',
        'NC State Wolfpack': 'NC State',
        'Duke Blue Devils': 'Duke',
        'Wake Forest Demon Deacons': 'Wake Forest',
        'Virginia Cavaliers': 'Virginia',
        'Virginia Tech Hokies': 'Virginia Tech',
        'Georgia Tech Yellow Jackets': 'Georgia Tech',
        'Pittsburgh Panthers': 'Pittsburgh',
        'Syracuse Orange': 'Syracuse',
        'Boston College Eagles': 'Boston College',
        'Louisville Cardinals': 'Louisville',
        
        # Notre Dame
        'Notre Dame Fighting Irish': 'Notre Dame',
        
        # Group of Five examples
        'Cincinnati Bearcats': 'Cincinnati',
        'Houston Cougars': 'Houston',
        'UCF Knights': 'UCF',
        'Memphis Tigers': 'Memphis',
        'SMU Mustangs': 'SMU',
        'Navy Midshipmen': 'Navy',
        'Army Black Knights': 'Army',
        'Air Force Falcons': 'Air Force',
        'Boise State Broncos': 'Boise State',
        'Fresno State Bulldogs': 'Fresno State',
        'San Diego State Aztecs': 'San Diego State',
        'Nevada Wolf Pack': 'Nevada',
        'UNLV Rebels': 'UNLV',
        'Colorado State Rams': 'Colorado State',
        'Wyoming Cowboys': 'Wyoming',
        'New Mexico Lobos': 'New Mexico',
        'Hawaii Rainbow Warriors': 'Hawaii',
        'San Jose State Spartans': 'San José State',
        
        # More teams
        'Appalachian State Mountaineers': 'Appalachian State',
        'Coastal Carolina Chanticleers': 'Coastal Carolina',
        'Liberty Flames': 'Liberty',
        'BYU Cougars': 'BYU',
        
        # Friday September 5, 2025 games
        'James Madison Dukes': 'James Madison',
        'Louisville Cardinals': 'Louisville',
        'Northern Illinois Huskies': 'Northern Illinois',
        'Maryland Terrapins': 'Maryland',
        'Western Illinois Leathernecks': 'Western Illinois',
        'Northwestern Wildcats': 'Northwestern',
        'Eastern Washington Eagles': 'Eastern Washington',
        'Boise State Broncos': 'Boise State',
        
        # Fix mappings for Friday backtest teams
        'FIU Panthers': 'Florida International',
        'Appalachian State Mountaineers': 'App State',
        'Charlotte 49ers': 'Charlotte',
        'Central Michigan Chippewas': 'Central Michigan',
        'Sam Houston Bearkats': 'Sam Houston',
        'Wagner Seahawks': 'Wagner Seahawks',  # FCS - will still use fallback
        'Kennesaw State': 'Kennesaw State',  # Already matches
        'Wake Forest Demon Deacons': 'Wake Forest',
        'Western Michigan Broncos': 'Western Michigan',
        'Tarleton State': 'Tarleton State',  # FCS - will still use fallback
        'Bethune-Cookman': 'Bethune-Cookman',  # FCS - will still use fallback
        
        # Additional Group of Five and FCS teams
        'Delaware State Hornets': 'Delaware State',
        'Howard Bison': 'Howard',
        'Montana State Bobcats': 'Montana State',
        'Murray State Racers': 'Murray State',
    }
    
    return mapping

def normalize_team_name(team_name: str, mapping: dict = None) -> str:
    """
    Normalize a team name to match embedding format.
    
    Args:
        team_name: Full team name (e.g. "Alabama Crimson Tide")
        mapping: Optional custom mapping dict
        
    Returns:
        Short team name (e.g. "Alabama")
    """
    if mapping is None:
        mapping = create_team_name_mapping()
    
    if team_name in mapping:
        return mapping[team_name]
    
    # If not in mapping, try to extract just the location/school name
    # This is a fallback for teams not in our mapping
    if team_name:
        # Common patterns to remove
        words_to_remove = [
            'Crimson Tide', 'Tigers', 'Bulldogs', 'Wildcats', 'Eagles', 'Cardinals',
            'Trojans', 'Bruins', 'Ducks', 'Huskies', 'Buckeyes', 'Wolverines',
            'Nittany Lions', 'Spartans', 'Badgers', 'Hawkeyes', 'Fighting Illini',
            'Boilermakers', 'Hoosiers', 'Cornhuskers', 'Longhorns', 'Sooners',
            'Cowboys', 'Jayhawks', 'Cyclones', 'Red Raiders', 'Bears', 'Horned Frogs',
            'Mountaineers', 'Sun Devils', 'Buffaloes', 'Cougars', 'Beavers',
            'Seminoles', 'Hurricanes', 'Tar Heels', 'Wolfpack', 'Blue Devils',
            'Demon Deacons', 'Cavaliers', 'Hokies', 'Yellow Jackets', 'Panthers',
            'Orange', 'Golden Bears', 'Cardinal', 'Utes', 'Golden Gophers',
            'Scarlet Knights', 'Terrapins', 'Volunteers', 'Gamecocks', 'Razorbacks',
            'Aggies', 'Commodores', 'Rebels', 'Bearcats', 'Knights', 'Mustangs',
            'Midshipmen', 'Black Knights', 'Falcons', 'Broncos', 'Aztecs',
            'Wolf Pack', 'Rams', 'Lobos', 'Rainbow Warriors', 'Chanticleers',
            'Flames', 'Hornets', 'Bison', 'Bobcats', 'Racers', 'Fighting Irish'
        ]
        
        # Try removing common mascot names
        for mascot in words_to_remove:
            if team_name.endswith(mascot):
                result = team_name[:-len(mascot)].strip()
                if result:
                    return result
    
    # If all else fails, return original name
    return team_name

def test_team_mapping():
    """Test the team name mapping function."""
    mapping = create_team_name_mapping()
    
    test_cases = [
        'Alabama Crimson Tide',
        'Ohio State Buckeyes', 
        'Oregon Ducks',
        'Florida State Seminoles',
        'Unknown Team Name'
    ]
    
    for team in test_cases:
        normalized = normalize_team_name(team, mapping)
        print(f"{team} -> {normalized}")

if __name__ == "__main__":
    test_team_mapping()