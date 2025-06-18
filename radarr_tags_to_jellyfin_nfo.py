#
# radarr_tags_to_jellyfin_nfo.py
# It preserves genre tags while ensuring only 'elsewherr' organizational tags are present.
#

import os
import yaml
import logging
import argparse
import xml.etree.ElementTree as ET
from pyarr import RadarrAPI
from xml.dom import minidom

# --- Script ---
def main():
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Sync Radarr tags to Jellyfin NFO files.")
    parser.add_argument('-v', '--verbose', action='store_true', help="Enable verbose DEBUG logging.")
    args = parser.parse_args()

    # --- Configuration ---
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
    TAG_PREFIX = config['prefix']

    # --- Logging Setup ---
    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_file_path = os.path.join(script_dir, 'radarr_to_jellyfin.log')
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler()
        ]
    )
    logging.info("--- Starting Radarr to Jellyfin NFO tag sync script ---")
    logging.debug(f"Verbose logging enabled.")

    try:
        radarr = RadarrAPI(RADARR_URL, RADARR_API_KEY)
        all_tags = radarr.get_tag()
        tags_id_to_label = {tag['id']: tag['label'] for tag in all_tags}
        logging.info("Successfully connected to Radarr.")
    except Exception as e:
        logging.error(f"Failed to connect to Radarr. Please check URL and API Key. Error: {e}")
        return

    movies = radarr.get_movie()
    logging.info(f"Found {len(movies)} movies to process.")
    logging.info(f"Filtering for tags with prefix: '{TAG_PREFIX}'")

    updated_count = 0
    for movie in movies:
        title = movie.get('title', 'Unknown Title')

        # **PATH FIX**: Get the path and strip any trailing slashes (forward or back)
        movie_path = movie.get('path', '').rstrip('/\\')

        if not movie_path:
            logging.warning(f"Skipping '{title}' as it has no path in Radarr.")
            continue

        nfo_file_path = os.path.join(movie_path, "movie.nfo")

        if not os.path.exists(nfo_file_path):
            # This log message will now be accurate
            logging.debug(f"No NFO file found for '{title}' at '{nfo_file_path}', skipping.")
            continue

        try:
            logging.info(f"Processing: {title}")
            all_radarr_labels = {tags_id_to_label.get(tag_id) for tag_id in movie.get('tags', []) if tags_id_to_label.get(tag_id)}
            elsewherr_labels_to_sync = {label for label in all_radarr_labels if label.startswith(TAG_PREFIX)}

            logging.debug(f"  - Radarr tags: {all_radarr_labels}")
            logging.debug(f"  - Tags to sync (prefix '{TAG_PREFIX}'): {elsewherr_labels_to_sync}")

            ET.register_namespace('', "http://www.w3.org/2001/XMLSchema-instance")
            tree = ET.parse(nfo_file_path)
            root = tree.getroot()

            current_nfo_tag_elements = root.findall('./tag')
            current_nfo_labels = {elem.text for elem in current_nfo_tag_elements if elem.text}
            logging.debug(f"  - Current NFO tags: {current_nfo_labels}")

            final_tag_labels = {label for label in current_nfo_labels if label not in all_radarr_labels}
            final_tag_labels.update(elsewherr_labels_to_sync)

            if final_tag_labels != current_nfo_labels:
                logging.info(f"  - Change detected for '{title}'. Updating NFO file.")
                logging.debug(f"  - Old tags: {current_nfo_labels}")
                logging.debug(f"  - New tags: {sorted(list(final_tag_labels))}")

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

                logging.info(f"Successfully updated tags for: {title}")
                updated_count += 1
            else:
                logging.info(f"  - No tag changes needed for '{title}'. Skipping update.")

        except ET.ParseError as e:
            logging.error(f"Could not parse XML for '{title}'s NFO file. Error: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred for '{title}': {e}")

    logging.info("--- Script finished ---")
    logging.info(f"Summary: Processed {len(movies)} movies, updated {updated_count} NFO files.")

if __name__ == '__main__':
    main()