#!/usr/bin/env python3
"""
MLB Team Logo Generator and Mapping Utility
Creates simple SVG logos for all MLB teams and provides mapping functions.
"""

import os
from pathlib import Path

# MLB Team mapping with official team names and PNG logos
MLB_TEAMS = {
    # American League East
    'Baltimore Orioles': {'abbr': 'BAL', 'color': '#DF4601', 'file': 'baltimore_orioles.png'},
    'Boston Red Sox': {'abbr': 'BOS', 'color': '#BD3039', 'file': 'boston_red_sox.png'},
    'New York Yankees': {'abbr': 'NYY', 'color': '#132448', 'file': 'new_york_yankees.png'},
    'Tampa Bay Rays': {'abbr': 'TB', 'color': '#092C5C', 'file': 'tampa_bay_rays.png'},
    'Toronto Blue Jays': {'abbr': 'TOR', 'color': '#134A8E', 'file': 'toronto_blue_jays.png'},
    
    # American League Central  
    'Chicago White Sox': {'abbr': 'CWS', 'color': '#27251F', 'file': 'chicago_white_sox.png'},
    'Cleveland Guardians': {'abbr': 'CLE', 'color': '#E31937', 'file': 'cleveland_guardians.png'},
    'Detroit Tigers': {'abbr': 'DET', 'color': '#0C2340', 'file': 'detroit_tigers.png'},
    'Kansas City Royals': {'abbr': 'KC', 'color': '#004687', 'file': 'kansas_city_royals.png'},
    'Minnesota Twins': {'abbr': 'MIN', 'color': '#002B5C', 'file': 'minnesota_twins.png'},
    
    # American League West
    'Houston Astros': {'abbr': 'HOU', 'color': '#002D62', 'file': 'houston_astros.png'},
    'Los Angeles Angels': {'abbr': 'LAA', 'color': '#BA0021', 'file': 'los_angeles_angels.png'},
    'Oakland Athletics': {'abbr': 'OAK', 'color': '#003831', 'file': 'oakland_athletics.png'},
    'Seattle Mariners': {'abbr': 'SEA', 'color': '#0C2C56', 'file': 'seattle_mariners.png'},
    'Texas Rangers': {'abbr': 'TEX', 'color': '#003278', 'file': 'texas_rangers.png'},
    
    # National League East
    'Atlanta Braves': {'abbr': 'ATL', 'color': '#CE1141', 'file': 'atlanta_braves.png'},
    'Miami Marlins': {'abbr': 'MIA', 'color': '#00A3E0', 'file': 'miami_marlins.png'},
    'New York Mets': {'abbr': 'NYM', 'color': '#002D72', 'file': 'new_york_mets.png'},
    'Philadelphia Phillies': {'abbr': 'PHI', 'color': '#E81828', 'file': 'philadelphia_phillies.png'},
    'Washington Nationals': {'abbr': 'WSH', 'color': '#AB0003', 'file': 'washington_nationals.png'},
    
    # National League Central
    'Chicago Cubs': {'abbr': 'CHC', 'color': '#0E3386', 'file': 'chicago_cubs.png'},
    'Cincinnati Reds': {'abbr': 'CIN', 'color': '#C6011F', 'file': 'cincinnati_reds.png'},
    'Milwaukee Brewers': {'abbr': 'MIL', 'color': '#12284B', 'file': 'milwaukee_brewers.png'},
    'Pittsburgh Pirates': {'abbr': 'PIT', 'color': '#27251F', 'file': 'pittsburgh_pirates.png'},
    'St. Louis Cardinals': {'abbr': 'STL', 'color': '#C41E3A', 'file': 'st_louis_cardinals.png'},
    
    # National League West
    'Arizona Diamondbacks': {'abbr': 'AZ', 'color': '#A71930', 'file': 'arizona_diamondbacks.png'},
    'Colorado Rockies': {'abbr': 'COL', 'color': '#33006F', 'file': 'colorado_rockies.png'},
    'Los Angeles Dodgers': {'abbr': 'LAD', 'color': '#005A9C', 'file': 'los_angeles_dodgers.png'},
    'San Diego Padres': {'abbr': 'SD', 'color': '#2F241D', 'file': 'san_diego_padres.png'},
    'San Francisco Giants': {'abbr': 'SF', 'color': '#FD5A1E', 'file': 'san_francisco_giants.png'},
}

def create_team_logo_svg(team_name, abbr, color, size=50):
    """Create a simple SVG logo for a team."""
    return f'''<svg width="{size}" height="{size}" viewBox="0 0 {size} {size}" xmlns="http://www.w3.org/2000/svg">
  <circle cx="{size//2}" cy="{size//2}" r="{size//2}" fill="{color}"/>
  <text x="{size//2}" y="{size//2 + 7}" font-family="Arial, sans-serif" font-size="{size//3}" font-weight="bold" fill="white" text-anchor="middle">{abbr}</text>
</svg>'''

def generate_all_logos(output_dir):
    """Generate SVG logos for all MLB teams."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    for team_name, team_info in MLB_TEAMS.items():
        logo_svg = create_team_logo_svg(team_name, team_info['abbr'], team_info['color'])
        logo_path = output_path / team_info['file']
        
        with open(logo_path, 'w') as f:
            f.write(logo_svg)
        
        print(f"Created logo: {logo_path}")

def get_team_logo_path(team_name):
    """Get the logo file path for a team name (handles variations)."""
    # Direct match
    if team_name in MLB_TEAMS:
        return f"/static/images/{MLB_TEAMS[team_name]['file']}"
    
    # Try to match by abbreviation
    for full_name, info in MLB_TEAMS.items():
        if info['abbr'].lower() == team_name.lower():
            return f"/static/images/{info['file']}"
    
    # Try partial name matching
    team_lower = team_name.lower()
    for full_name, info in MLB_TEAMS.items():
        if any(word in full_name.lower() for word in team_lower.split()):
            return f"/static/images/{info['file']}"
    
    # Default fallback
    return "/static/images/default_team.png"

if __name__ == "__main__":
    # Generate all logos when run directly
    script_dir = Path(__file__).parent
    output_dir = script_dir / "static" / "images"
    generate_all_logos(output_dir)
    print(f"Generated {len(MLB_TEAMS)} team logos in {output_dir}")