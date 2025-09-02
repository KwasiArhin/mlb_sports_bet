#!/usr/bin/env python3
"""
Download real MLB team logos from reliable sources
"""

import requests
import os
from pathlib import Path
import time

# Real MLB team logos from reliable CDNs and sources
MLB_TEAM_LOGOS = {
    # Using ESPN and other reliable sources
    'arizona_diamondbacks': 'https://a.espncdn.com/i/teamlogos/mlb/500/ari.png',
    'atlanta_braves': 'https://a.espncdn.com/i/teamlogos/mlb/500/atl.png',
    'baltimore_orioles': 'https://a.espncdn.com/i/teamlogos/mlb/500/bal.png',
    'boston_red_sox': 'https://a.espncdn.com/i/teamlogos/mlb/500/bos.png',
    'chicago_cubs': 'https://a.espncdn.com/i/teamlogos/mlb/500/chc.png',
    'chicago_white_sox': 'https://a.espncdn.com/i/teamlogos/mlb/500/cws.png',
    'cincinnati_reds': 'https://a.espncdn.com/i/teamlogos/mlb/500/cin.png',
    'cleveland_guardians': 'https://a.espncdn.com/i/teamlogos/mlb/500/cle.png',
    'colorado_rockies': 'https://a.espncdn.com/i/teamlogos/mlb/500/col.png',
    'detroit_tigers': 'https://a.espncdn.com/i/teamlogos/mlb/500/det.png',
    'houston_astros': 'https://a.espncdn.com/i/teamlogos/mlb/500/hou.png',
    'kansas_city_royals': 'https://a.espncdn.com/i/teamlogos/mlb/500/kc.png',
    'los_angeles_angels': 'https://a.espncdn.com/i/teamlogos/mlb/500/laa.png',
    'los_angeles_dodgers': 'https://a.espncdn.com/i/teamlogos/mlb/500/lad.png',
    'miami_marlins': 'https://a.espncdn.com/i/teamlogos/mlb/500/mia.png',
    'milwaukee_brewers': 'https://a.espncdn.com/i/teamlogos/mlb/500/mil.png',
    'minnesota_twins': 'https://a.espncdn.com/i/teamlogos/mlb/500/min.png',
    'new_york_mets': 'https://a.espncdn.com/i/teamlogos/mlb/500/nym.png',
    'new_york_yankees': 'https://a.espncdn.com/i/teamlogos/mlb/500/nyy.png',
    'oakland_athletics': 'https://a.espncdn.com/i/teamlogos/mlb/500/oak.png',
    'philadelphia_phillies': 'https://a.espncdn.com/i/teamlogos/mlb/500/phi.png',
    'pittsburgh_pirates': 'https://a.espncdn.com/i/teamlogos/mlb/500/pit.png',
    'san_diego_padres': 'https://a.espncdn.com/i/teamlogos/mlb/500/sd.png',
    'san_francisco_giants': 'https://a.espncdn.com/i/teamlogos/mlb/500/sf.png',
    'seattle_mariners': 'https://a.espncdn.com/i/teamlogos/mlb/500/sea.png',
    'st_louis_cardinals': 'https://a.espncdn.com/i/teamlogos/mlb/500/stl.png',
    'tampa_bay_rays': 'https://a.espncdn.com/i/teamlogos/mlb/500/tb.png',
    'texas_rangers': 'https://a.espncdn.com/i/teamlogos/mlb/500/tex.png',
    'toronto_blue_jays': 'https://a.espncdn.com/i/teamlogos/mlb/500/tor.png',
    'washington_nationals': 'https://a.espncdn.com/i/teamlogos/mlb/500/wsh.png',
}

def download_real_logo(team_name, url, output_dir):
    """Download a real team logo from URL."""
    try:
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        
        # Check if we got an actual image (should be larger than 1KB)
        if len(response.content) < 1000:
            print(f"Warning: {team_name} logo seems too small ({len(response.content)} bytes)")
            return False
        
        logo_path = output_path / f"{team_name}.png"
        with open(logo_path, 'wb') as f:
            f.write(response.content)
        
        print(f"✓ Downloaded: {team_name}.png ({len(response.content)} bytes)")
        return True
        
    except Exception as e:
        print(f"✗ Failed to download {team_name}: {e}")
        return False

def download_all_real_logos(output_dir):
    """Download all real MLB team logos."""
    success_count = 0
    
    print("Downloading official MLB team logos from ESPN...")
    print("=" * 50)
    
    for team_name, url in MLB_TEAM_LOGOS.items():
        if download_real_logo(team_name, url, output_dir):
            success_count += 1
        # Add a small delay to be respectful to the server
        time.sleep(0.5)
    
    print("=" * 50)
    print(f"Successfully downloaded {success_count}/{len(MLB_TEAM_LOGOS)} team logos!")
    
    return success_count == len(MLB_TEAM_LOGOS)

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    output_dir = script_dir / "static" / "images"
    download_all_real_logos(output_dir)