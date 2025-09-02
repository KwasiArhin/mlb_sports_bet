#!/usr/bin/env python3
"""
Download official MLB team logos in PNG format
"""

import requests
import os
from pathlib import Path

# MLB team logo URLs - using reliable sources with official logos
TEAM_LOGOS = {
    # American League East
    'baltimore_orioles': 'https://logos-world.net/wp-content/uploads/2020/05/Baltimore-Orioles-Logo.png',
    'boston_red_sox': 'https://logos-world.net/wp-content/uploads/2020/05/Boston-Red-Sox-Logo.png',
    'new_york_yankees': 'https://logos-world.net/wp-content/uploads/2020/05/New-York-Yankees-Logo.png',
    'tampa_bay_rays': 'https://logos-world.net/wp-content/uploads/2020/05/Tampa-Bay-Rays-Logo.png',
    'toronto_blue_jays': 'https://logos-world.net/wp-content/uploads/2020/05/Toronto-Blue-Jays-Logo.png',
    
    # American League Central
    'chicago_white_sox': 'https://logos-world.net/wp-content/uploads/2020/05/Chicago-White-Sox-Logo.png',
    'cleveland_guardians': 'https://logos-world.net/wp-content/uploads/2021/12/Cleveland-Guardians-Logo.png',
    'detroit_tigers': 'https://logos-world.net/wp-content/uploads/2020/05/Detroit-Tigers-Logo.png',
    'kansas_city_royals': 'https://logos-world.net/wp-content/uploads/2020/05/Kansas-City-Royals-Logo.png',
    'minnesota_twins': 'https://logos-world.net/wp-content/uploads/2020/05/Minnesota-Twins-Logo.png',
    
    # American League West
    'houston_astros': 'https://logos-world.net/wp-content/uploads/2020/05/Houston-Astros-Logo.png',
    'los_angeles_angels': 'https://logos-world.net/wp-content/uploads/2020/05/Los-Angeles-Angels-Logo.png',
    'oakland_athletics': 'https://logos-world.net/wp-content/uploads/2020/05/Oakland-Athletics-Logo.png',
    'seattle_mariners': 'https://logos-world.net/wp-content/uploads/2020/05/Seattle-Mariners-Logo.png',
    'texas_rangers': 'https://logos-world.net/wp-content/uploads/2020/05/Texas-Rangers-Logo.png',
    
    # National League East
    'atlanta_braves': 'https://logos-world.net/wp-content/uploads/2020/05/Atlanta-Braves-Logo.png',
    'miami_marlins': 'https://logos-world.net/wp-content/uploads/2020/05/Miami-Marlins-Logo.png',
    'new_york_mets': 'https://logos-world.net/wp-content/uploads/2020/05/New-York-Mets-Logo.png',
    'philadelphia_phillies': 'https://logos-world.net/wp-content/uploads/2020/05/Philadelphia-Phillies-Logo.png',
    'washington_nationals': 'https://logos-world.net/wp-content/uploads/2020/05/Washington-Nationals-Logo.png',
    
    # National League Central
    'chicago_cubs': 'https://logos-world.net/wp-content/uploads/2020/05/Chicago-Cubs-Logo.png',
    'cincinnati_reds': 'https://logos-world.net/wp-content/uploads/2020/05/Cincinnati-Reds-Logo.png',
    'milwaukee_brewers': 'https://logos-world.net/wp-content/uploads/2020/05/Milwaukee-Brewers-Logo.png',
    'pittsburgh_pirates': 'https://logos-world.net/wp-content/uploads/2020/05/Pittsburgh-Pirates-Logo.png',
    'st_louis_cardinals': 'https://logos-world.net/wp-content/uploads/2020/05/St.-Louis-Cardinals-Logo.png',
    
    # National League West
    'arizona_diamondbacks': 'https://logos-world.net/wp-content/uploads/2020/05/Arizona-Diamondbacks-Logo.png',
    'colorado_rockies': 'https://logos-world.net/wp-content/uploads/2020/05/Colorado-Rockies-Logo.png',
    'los_angeles_dodgers': 'https://logos-world.net/wp-content/uploads/2020/05/Los-Angeles-Dodgers-Logo.png',
    'san_diego_padres': 'https://logos-world.net/wp-content/uploads/2020/05/San-Diego-Padres-Logo.png',
    'san_francisco_giants': 'https://logos-world.net/wp-content/uploads/2020/05/San-Francisco-Giants-Logo.png',
}

def download_logo(team_name, url, output_dir):
    """Download a team logo from URL."""
    try:
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        response = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        response.raise_for_status()
        
        logo_path = output_path / f"{team_name}.png"
        with open(logo_path, 'wb') as f:
            f.write(response.content)
        
        print(f"Downloaded: {team_name}.png")
        return True
        
    except Exception as e:
        print(f"Failed to download {team_name}: {e}")
        return False

def download_all_logos(output_dir):
    """Download all MLB team logos."""
    success_count = 0
    
    for team_name, url in TEAM_LOGOS.items():
        if download_logo(team_name, url, output_dir):
            success_count += 1
    
    print(f"\nDownloaded {success_count}/{len(TEAM_LOGOS)} team logos successfully!")

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    output_dir = script_dir / "static" / "images"
    download_all_logos(output_dir)