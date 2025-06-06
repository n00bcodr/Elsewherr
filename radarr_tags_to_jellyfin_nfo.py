import os
import yaml
import xml.etree.ElementTree as ET
from pyarr import RadarrAPI

try:
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    print("FATAL: config.yaml not found. Please place it in the same directory as this script.")
    exit()

RADARR_URL = config['radarr']['url']
RADARR_API_KEY = config['radarr']['api_key']
TAG_PREFIX = config.get('prefix', 'elsewherr(in)-')

radarr = RadarrAPI(RADARR_URL, RADARR_API_KEY)
all_tags = radarr.get_tag()
tags_id_to_label = {tag['id']: tag['label'] for tag in all_tags}

movies = radarr.get_movie()
print(f"Found {len(movies)} movies to process.")
print(f"Filtering for tags with prefix: '{TAG_PREFIX}'")

for movie in movies:
    movie_path = movie['path']
    movie_tags_ids = movie.get('tags', [])

    if not movie_tags_ids:
        continue

    all_movie_tag_labels = [tags_id_to_label.get(tag_id) for tag_id in movie_tags_ids]

    filtered_tag_labels = [label for label in all_movie_tag_labels if label and label.startswith(TAG_PREFIX)]

    if not filtered_tag_labels:

        continue

    nfo_file_path = os.path.join(movie_path, "movie.nfo")
    print(f"nfo_file_path : {nfo_file_path}")

    if not os.path.exists(nfo_file_path):
        print(f"Warning: Could not find NFO file for {movie['title']} in {movie_path}")
        continue

    try:
        ET.register_namespace('', "http://www.w3.org/2001/XMLSchema-instance")
        tree = ET.parse(nfo_file_path)
        root = tree.getroot()

        existing_nfo_tags_to_remove = [
            tag_element for tag_element in root.findall('./tag')
            if tag_element.text and tag_element.text.startswith(TAG_PREFIX)
        ]

        for tag_element in existing_nfo_tags_to_remove:
            root.remove(tag_element)

        for tag_label in filtered_tag_labels:
            if not any(tag.text == tag_label for tag in root.findall('./tag')):
                new_tag = ET.Element('tag')
                new_tag.text = tag_label
                root.append(new_tag)

        tree.write(nfo_file_path, encoding='utf-8', xml_declaration=True)
        print(f"Updated tags for: {movie['title']} -> {', '.join(filtered_tag_labels)}")

    except ET.ParseError:
        print(f"Error: Could not parse XML for {movie['title']}'s NFO file.")
    except Exception as e:
        print(f"An unexpected error occurred for {movie['title']}: {e}")

print("Script finished.")