#
# sonarr_tags_to_jellyfin_nfo.py
# It preserves existing tags while ensuring only 'elsewherr' organizational tags are present.
#

import os
import yaml
import logging
import argparse
import xml.etree.ElementTree as ET
from pyarr import SonarrAPI
from xml.dom import minidom

# --- Script ---
def main():
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Sync Sonarr tags to Jellyfin NFO files.")
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

    SONARR_URL = config['sonarr']['url']
    SONARR_API_KEY = config['sonarr']['api_key']
    TAG_PREFIX = config['prefix']

    # --- Logging Setup ---
    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_file_path = os.path.join(script_dir, 'sonarr_to_jellyfin.log')
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, mode='w'),
            logging.StreamHandler()
        ]
    )

    logging.info("--- Starting Sonarr to Jellyfin NFO tag sync script ---")
    logging.debug("Verbose logging enabled.")

    try:
        sonarr = SonarrAPI(SONARR_URL, SONARR_API_KEY)
        all_tags = sonarr.get_tag()
        tags_id_to_label = {tag['id']: tag['label'] for tag in all_tags}
        logging.info("Successfully connected to Sonarr.")
    except Exception as e:
        logging.error(f"Failed to connect to Sonarr. Please check URL and API Key. Error: {e}")
        return

    series_list = sonarr.get_series()
    logging.info(f"Found {len(series_list)} series to process.")
    logging.info(f"Filtering for tags with prefix: '{TAG_PREFIX}'")

    updated_count = 0
    for series in series_list:
        title = series.get('title', 'Unknown Title')

        # **PATH FIX**: Get the path and strip any trailing slashes (forward or back)
        series_path = series.get('path', '').rstrip('/\\')

        if not series_path:
            logging.warning(f"Skipping '{title}' as it has no path in Sonarr.")
            continue

        nfo_file_path = os.path.join(series_path, "tvshow.nfo")

        if not os.path.exists(nfo_file_path):
            logging.debug(f"No NFO file found for '{title}' at '{nfo_file_path}', skipping.")
            continue

        try:
            logging.info(f"Processing: {title}")

            all_sonarr_labels = {tags_id_to_label.get(tag_id) for tag_id in series.get('tags', []) if tags_id_to_label.get(tag_id)}
            elsewherr_labels_to_sync = {label for label in all_sonarr_labels if label.startswith(TAG_PREFIX)}

            logging.debug(f"  - Sonarr tags to sync (prefix '{TAG_PREFIX}'): {elsewherr_labels_to_sync}")

            ET.register_namespace('', "http://www.w3.org/2001/XMLSchema-instance")
            tree = ET.parse(nfo_file_path)
            root = tree.getroot()

            current_nfo_tags_with_prefix = {
                elem.text for elem in root.findall('./tag')
                if elem.text and elem.text.startswith(TAG_PREFIX)
            }

            if current_nfo_tags_with_prefix == elsewherr_labels_to_sync:
                logging.info(f"  - No tag changes needed for '{title}'. Skipping update.")
                continue

            logging.info(f"  - Change detected for '{title}'. Updating NFO file.")
            logging.debug(f"  - Old prefixed tags in NFO: {current_nfo_tags_with_prefix}")
            logging.debug(f"  - New prefixed tags from Sonarr: {elsewherr_labels_to_sync}")

            tags_to_remove = [
                tag_element for tag_element in root.findall('./tag')
                if tag_element.text and tag_element.text.startswith(TAG_PREFIX)
            ]
            for tag_element in tags_to_remove:
                root.remove(tag_element)

            for label in sorted(list(elsewherr_labels_to_sync)):
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

        except ET.ParseError as e:
            logging.error(f"Could not parse XML for '{title}'s NFO file. Error: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred for '{title}': {e}")

    logging.info("--- Script finished ---")
    logging.info(f"Summary: Processed {len(series_list)} series, updated {updated_count} NFO files.")

if __name__ == '__main__':
    main()