"""
Elsewherr - A script to automatically tag Sonarr/Radarr media
with streaming provider information from TMDb, now with parallel processing.
"""

import argparse
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
from gotify import Gotify
from pyarr import RadarrAPI, SonarrAPI
from requests.exceptions import RequestException
from tmdbv3api import TMDb, Find, Movie, TV

# --- Constants ---
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 30
# Set the number of parallel threads to run.
# 10 is a safe number to avoid hitting the TMDB API rate limit (~50 req/sec).
MAX_WORKERS = 20


class Elsewherr:
    """
    A class to encapsulate the logic for tagging media in Radarr and Sonarr.
    """

    def __init__(self, args: argparse.Namespace):
        """
        Initializes the Elsewherr application.
        """
        self.base_dir = Path(__file__).resolve().parent
        self.logger = self._setup_logging(args)
        self.logger.info("Elsewherr is starting.")

        self.config = self._load_config()
        self.tmdb = self._setup_tmdb()
        self.gotify = self._setup_gotify()

        self.prefix = self.config.get("prefix", "svcp-")
        self.providers = self.config.get("providers", [])
        self.region = self.config.get("tmdb", {}).get("region", "US")

    def _setup_logging(self, args: argparse.Namespace) -> logging.Logger:
        """
        Configures logging based on command-line arguments.
        """
        log_level = logging.DEBUG if args.verbose else logging.INFO
        log_file = self.base_dir / "logs" / "elsewherr.log" if args.log_to_file else None

        if log_file:
            log_file.parent.mkdir(exist_ok=True)

        logging.basicConfig(
            level=log_level,
            format="%(asctime)s :: %(levelname)-8s :: %(threadName)s :: %(message)s",
            handlers=[
                logging.StreamHandler(),
                *(logging.FileHandler(log_file) for f in [log_file] if f),
            ],
        )

        # Quieten noisy third-party loggers to prevent log spam in verbose mode.
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

        logger = logging.getLogger(__name__)
        if args.verbose:
            logger.debug("Verbose logging enabled.")
        return logger

    def _load_config(self) -> Dict[str, Any]:
        """
        Loads the configuration from config.yaml.
        """
        config_path = self.base_dir / "config.yaml"
        self.logger.debug(f"Loading configuration from {config_path}")
        try:
            with open(config_path, "r") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found at {config_path}. Exiting.")
            exit(1)
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML configuration: {e}. Exiting.")
            exit(1)

    def _setup_tmdb(self) -> TMDb:
        """Initializes and returns a configured TMDb API client."""
        tmdb_api_key = self.config.get("tmdb", {}).get("api_key")
        if not tmdb_api_key:
            self.logger.error("TMDb API key is missing from config.yaml. Exiting.")
            exit(1)
        tmdb = TMDb()
        tmdb.api_key = tmdb_api_key
        return tmdb

    def _setup_gotify(self) -> Optional[Gotify]:
        """Initializes and returns a Gotify client if configured."""
        if self.config.get("gotify", {}).get("enabled"):
            self.logger.debug("Gotify notifications enabled.")
            gotify_config = self.config["gotify"]
            return Gotify(base_url=gotify_config["url"], app_token=gotify_config["token"])
        self.logger.debug("Gotify notifications disabled.")
        return None

    def _get_tag_label_for_provider(self, provider_name: str) -> str:
        """
        Generates a standardized tag label from a provider name.
        """
        sanitized_name = re.sub("[^A-Za-z0-9]+", "", provider_name)
        return f"{self.prefix}{sanitized_name}".lower()

    def send_notification(self, title: str, message: str, priority: int = 1):
        """Sends a notification via Gotify if it's enabled."""
        if self.gotify:
            try:
                self.logger.debug(f"Sending notification: '{title}' - '{message}'")
                self.gotify.create_message(message, title=title, priority=priority)
            except Exception as e:
                self.logger.error(f"Failed to send Gotify notification: {e}")

    def _process_single_item(self, item: Dict[str, Any], media_type: str, service_name: str, api_client: Any, tags_id_to_label: Dict, tags_label_to_id: Dict) -> bool:
        """
        Processes a single media item. This function is designed to be run in a thread.

        Returns:
            True if the item was processed successfully (even if no changes were made),
            False if a permanent error occurred.
        """
        title = item["title"]
        # self.logger.debug(f"Processing {media_type}: {title} (ID: {item['id']})")

        # Define service-specific functions
        get_providers_func = Movie().watch_providers if media_type == "movie" else TV().watch_providers
        update_media_func = api_client.upd_movie if media_type == "movie" else api_client.upd_series

        for attempt in range(MAX_RETRIES):
            try:
                # Get TMDB ID
                tmdb_id = None
                if media_type == "movie":
                    tmdb_id = item.get("tmdbId")
                elif media_type == "series":
                    tvdb_id = item.get("tvdbId")
                    if tvdb_id:
                        find_results = Find().find_by_tvdb_id(str(tvdb_id))
                        if find_results.get("tv_results"):
                            tmdb_id = find_results["tv_results"][0]["id"]

                if not tmdb_id:
                    self.logger.warning(f"Could not find TMDB ID for '{title}'. Skipping.")
                    return False

                # Get watch providers from TMDb
                providers_obj = get_providers_func(tmdb_id)
                results = getattr(providers_obj, "results", {})
                flatrate_providers = results.get(self.region, {}).get("flatrate", [])

                # Preserve existing tags that don't match our prefix
                current_tags_ids = set(item.get("tags", []))
                new_tags_ids = {
                    tag_id for tag_id in current_tags_ids
                    if not tags_id_to_label.get(tag_id, "").startswith(self.prefix)
                }

                # Add tags for available streaming providers
                for provider in flatrate_providers:
                    provider_name = provider.get("provider_name")
                    if provider_name in self.providers:
                        tag_label = self._get_tag_label_for_provider(provider_name)
                        tag_id = tags_label_to_id.get(tag_label)
                        if tag_id:
                            new_tags_ids.add(tag_id)

                # Compare and update if necessary
                if current_tags_ids != new_tags_ids:
                    # Construct a detailed message for logging and notification
                    added_labels = {tags_id_to_label.get(t) for t in new_tags_ids - current_tags_ids if t}
                    removed_labels = {tags_id_to_label.get(t) for t in current_tags_ids - new_tags_ids if t}

                    change_parts = []
                    if removed_labels:
                        change_parts.append(f"Removed: {', '.join(sorted(removed_labels))}")
                    if added_labels:
                        change_parts.append(f"Added: {', '.join(sorted(added_labels))}")

                    change_summary = ". ".join(change_parts)
                    self.logger.info(f"Updating tags for '{title}': {change_summary}")

                    # Update the item and send to the API
                    item["tags"] = list(new_tags_ids)
                    update_media_func(item)

                    # Send notification with the same summary
                    self.send_notification(f"{service_name}: {title}", change_summary)
                else:
                    self.logger.debug(f"No tag changes needed for '{title}'.")

                return True  # Success

            except RequestException as e:
                self.logger.warning(f"Network error for '{title}' (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    self.logger.error(f"Failed to process '{title}' after {MAX_RETRIES} attempts. Skipping.")
                    return False
            except Exception as e:
                self.logger.error(f"An unexpected error occurred while processing '{title}': {e}", exc_info=True)
                return False # Break loop on non-network errors
        return False


    def _process_service(
        self,
        service_name: str,
        api_client: Any,
        media_type: str,
    ):
        """
        Processes a media service (Radarr or Sonarr) using a thread pool.
        """
        self.logger.info(f"--- Processing {service_name} ---")

        # 1. Get all media items from the service
        try:
            get_media_func = api_client.get_movie if media_type == "movie" else api_client.get_series
            all_media = get_media_func()
        except Exception as e:
            self.logger.error(f"Failed to get media from {service_name}: {e}")
            return

        # 2. Ensure all required provider tags exist
        self.logger.debug(f"Ensuring provider tags exist in {service_name}...")
        for provider in self.providers:
            api_client.create_tag(self._get_tag_label_for_provider(provider))

        # 3. Create mappings for tag IDs and labels
        all_tags = api_client.get_tag()
        tags_id_to_label = {tag["id"]: tag["label"] for tag in all_tags}
        tags_label_to_id = {tag["label"]: tag["id"] for tag in all_tags}

        # 4. Process each media item in parallel
        processed_count = 0
        total_count = len(all_media)
        self.logger.info(f"Found {total_count} {media_type} items. Processing with {MAX_WORKERS} workers...")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix=f'{service_name}Worker') as executor:
            # Submit all items to the thread pool
            future_to_item = {
                executor.submit(self._process_single_item, item, media_type, service_name, api_client, tags_id_to_label, tags_label_to_id): item
                for item in all_media
            }

            # Process results as they complete
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    success = future.result()
                    if success:
                        processed_count += 1
                except Exception as e:
                    self.logger.error(f"Error processing '{item['title']}': {e}", exc_info=True)

        self.logger.info(f"--- Finished processing {service_name}: {processed_count}/{total_count} items processed successfully. ---")
        return processed_count

    def run(self):
        """
        The main execution method for the script.
        """
        summaries = []

        if self.config.get("radarr", {}).get("enabled"):
            radarr_config = self.config["radarr"]
            try:
                radarr_api = RadarrAPI(host_url=radarr_config["url"], api_key=radarr_config["api_key"])
                count = self._process_service("Radarr", radarr_api, "movie")
                summaries.append(f"{count} movies")
            except Exception as e:
                self.logger.error(f"Failed to initialize Radarr API client: {e}")

        if self.config.get("sonarr", {}).get("enabled"):
            sonarr_config = self.config["sonarr"]
            try:
                sonarr_api = SonarrAPI(host_url=sonarr_config["url"], api_key=sonarr_config["api_key"])
                count = self._process_service("Sonarr", sonarr_api, "series")
                summaries.append(f"{count} series")
            except Exception as e:
                self.logger.error(f"Failed to initialize Sonarr API client: {e}")

        if summaries:
            summary_message = f"Processed {' & '.join(summaries)}"
            self.send_notification("Elsewherr Run Complete", summary_message)

        self.logger.info("Elsewherr has finished.")


def main():
    """
    Main entry point for the script.
    """
    parser = argparse.ArgumentParser(description="Tag Radarr/Sonarr media with streaming provider info.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose (DEBUG level) logging.")
    parser.add_argument("-l", "--log-to-file", action="store_true", help="Enable logging to a file in the 'logs' directory.")
    args = parser.parse_args()

    app = Elsewherr(args)
    app.run()


if __name__ == "__main__":
    main()
