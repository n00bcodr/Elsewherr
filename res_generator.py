import requests
import os
import yaml
import argparse

script_directory = os.path.dirname(os.path.abspath(__file__))

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Generate provider and region resource files from TMDb.")
parser.add_argument("-c", "--config", type=str, default="config.yaml", help="Path to config file (default: config.yaml)")
args = parser.parse_args()

config_path = args.config if os.path.isabs(args.config) else os.path.join(script_directory, args.config)
config = yaml.safe_load(open(config_path))

if not config['tmdb']['api_key']:
    raise ImportError(name='config.yaml')

tmdbHeaders = {'Content-Type': 'application/json'}

tmdbResponseRegions = requests.get('https://api.themoviedb.org/3/watch/providers/regions?api_key='+config['tmdb']['api_key'], headers=tmdbHeaders)
tmdbRegions = tmdbResponseRegions.json()

tmdbResponseProviders = requests.get('https://api.themoviedb.org/3/watch/providers/movie?api_key='+config['tmdb']['api_key'], headers=tmdbHeaders)
tmdbProviders = tmdbResponseProviders.json()

with open(os.path.join(script_directory, 'res', 'regions.txt'), 'w', encoding='utf-8') as f:
    for result in tmdbRegions['results']:
        f.write(f"{result['iso_3166_1']}\t{result['english_name']}\n")

with open(os.path.join(script_directory, 'res', 'providers.txt'), 'w', encoding='utf-8') as f:
    for result in sorted(set(p['provider_name'] for p in tmdbProviders['results'])):
        f.write(f"{result}\n")
