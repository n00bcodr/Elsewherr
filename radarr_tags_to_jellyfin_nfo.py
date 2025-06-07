#
# radarr_tags_to_jellyfin_nfo.py
# It preserves genre tags while ensuring only 'elsewherr' organizational tags are present.
#

import os
import yaml
import xml.etree.ElementTree as ET
from pyarr import RadarrAPI
from xml.dom import minidom

# --- Configuration ---
# Ensure 'config.yaml' is in the same directory as this script.
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    print(f"FATAL: config.yaml not found at {config_path}. Please place it in the same directory as this script.")
    exit()

RADARR_URL = config['radarr']['url']
RADARR_API_KEY = config['radarr']['api_key']
TAG_PREFIX = config.get('prefix', 'elsewherr(in)-')

# --- Script ---
print("Connecting to Radarr...")
radarr = RadarrAPI(RADARR_URL, RADARR_API_KEY)
all_tags = radarr.get_tag()
tags_id_to_label = {tag['id']: tag['label'] for tag in all_tags}

movies = radarr.get_movie()
print(f"Found {len(movies)} movies to process.")
print(f"Filtering for tags with prefix: '{TAG_PREFIX}'")

for movie in movies:
    movie_path = movie['path']

    all_radarr_labels = {tags_id_to_label.get(tag_id) for tag_id in movie.get('tags', []) if
                         tags_id_to_label.get(tag_id)}

    elsewherr_labels_to_sync = {label for label in all_radarr_labels if label.startswith(TAG_PREFIX)}

    nfo_file_path = os.path.join(movie_path, "movie.nfo")

    if not os.path.exists(nfo_file_path):
        continue

    try:
        ET.register_namespace('', "http://www.w3.org/2001/XMLSchema-instance")
        tree = ET.parse(nfo_file_path)
        root = tree.getroot()

        current_nfo_tag_elements = root.findall('./tag')
        current_nfo_labels = {elem.text for elem in current_nfo_tag_elements if elem.text}

        final_tag_labels = set()

        for label in current_nfo_labels:
            if label not in all_radarr_labels:
                final_tag_labels.add(label)

        final_tag_labels.update(elsewherr_labels_to_sync)

        if final_tag_labels != current_nfo_labels:
            for tag_element in current_nfo_tag_elements:
                root.remove(tag_element)
            for label in sorted(list(final_tag_labels)):
                new_tag = ET.Element('tag')
                new_tag.text = label
                root.append(new_tag)
            rough_string = ET.tostring(root, 'utf-8')
            reparsed = minidom.parseString(rough_string)
            pretty_xml_string = reparsed.toprettyxml(indent="  ", encoding="utf-8")

            with open(nfo_file_path, 'wb') as f:
                f.write(pretty_xml_string)

            print(f"Updated tags for: {movie['title']}")

    except ET.ParseError as e:
        print(f"Error: Could not parse XML for {movie['title']}'s NFO file. Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred for {movie['title']}: {e}")

print("Script finished.")
