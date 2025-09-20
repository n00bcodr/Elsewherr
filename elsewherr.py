"""
Elsewherr - A script to automatically tag Sonarr/Radarr media
with streaming provider information from TMDb, now with parallel processing
and enhanced tabular output.
"""

import argparse
import logging
import os
import re
import sys
import time
import threading
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime

import yaml
from gotify import Gotify
from pyarr import RadarrAPI, SonarrAPI
from requests.exceptions import RequestException
from tmdbv3api import TMDb, Find, Movie, TV
from tabulate import tabulate
from tqdm import tqdm

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

        self.config = self._load_config()
        self.tmdb = self._setup_tmdb()
        self.gotify = self._setup_gotify()

        self.prefix = self.config.get("prefix", "svcp-")
        self.providers = self.config.get("providers", [])
        self.region = self.config.get("tmdb", {}).get("region", "US")

        # Add tracking for changes and errors
        self.changes_lock = threading.Lock()
        self.changes_log = []
        self.errors_log = [] # To store items that failed or were skipped
        self.service_stats = defaultdict(lambda: {'processed': 0, 'updated': 0, 'errors': 0})

    def _setup_logging(self, args: argparse.Namespace) -> logging.Logger:
        """
        Configures logging based on command-line arguments.
        """
        log_level = logging.DEBUG if args.verbose else logging.INFO
        log_file = self.base_dir / "logs" / "elsewherr.log" if args.log_to_file else None

        if log_file:
            log_file.parent.mkdir(exist_ok=True)

        # Create a custom formatter that doesn't interfere with progress bars
        formatter = logging.Formatter("%(levelname)-8s :: %(message)s")

        # Create handlers
        handlers = []

        # Only add console handler if not running with progress bars
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        handlers.append(console_handler)

        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)

        logging.basicConfig(
            level=log_level,
            handlers=handlers,
        )

        # Quieten noisy third-party loggers to prevent log spam and progress bar interference
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("tmdbv3api").setLevel(logging.WARNING)
        logging.getLogger("pyarr").setLevel(logging.WARNING)

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

    def _log_change(self, service_name: str, title: str, added_labels: set, removed_labels: set, success: bool = True):
        """
        Thread-safe logging of changes with structured data collection.
        """
        change_parts = []
        if removed_labels:
            change_parts.append(f"❌ {', '.join(sorted(removed_labels))}")
        if added_labels:
            change_parts.append(f"✅ {', '.join(sorted(added_labels))}")

        change_summary = " | ".join(change_parts) if change_parts else "No changes"

        # Thread-safe logging
        with self.changes_lock:
            self.changes_log.append({
                'service': service_name,
                'title': title[:30] + "..." if len(title) > 30 else title,  # Truncate long titles
                'changes': change_summary,
            })

            if success:
                self.service_stats[service_name]['processed'] += 1
                if change_parts:  # Only count as updated if there were actual changes
                    self.service_stats[service_name]['updated'] += 1
            else:
                self.service_stats[service_name]['errors'] += 1

    def _print_changes_summary(self):
        """
        Print neat tabular summaries of all changes and issues encountered.
        """
        # --- 1. Changes Summary ---
        actual_changes = [change for change in self.changes_log if "No changes" not in change['changes']]

        if actual_changes:
            print("\n" + "="*100)
            print("📊 CHANGES SUMMARY")
            print("="*100)

            table_data = [
                [change['service'], change['title'], change['changes']]
                for change in actual_changes
            ]
            headers = ['Service', 'Title', 'Changes']
            print(tabulate(table_data, headers=headers, tablefmt='grid', maxcolwidths=[8, 35, 55]))
        else:
            print("\nNo tag changes were required during this run.")

        # --- 2. Issues Summary ---
        if self.errors_log:
            print("\n" + "="*100)
            print("⚠️ ISSUES SUMMARY")
            print("="*100)
            errors_table = [
                [error['service'], error['title'], error['reason']]
                for error in sorted(self.errors_log, key=lambda x: x['service'])
            ]
            errors_headers = ['Service', 'Title', 'Reason']
            print(tabulate(errors_table, headers=errors_headers, tablefmt='grid', maxcolwidths=[8, 35, 55]))

        # --- 3. Service Statistics ---
        print("\n" + "="*60)
        print("📈 SERVICE STATISTICS")
        print("="*60)

        stats_table = []
        for service, stats in self.service_stats.items():
            stats_table.append([
                service,
                stats['processed'],
                stats['updated'],
                stats['errors'],
                f"{(stats['updated']/stats['processed']*100):.1f}%" if stats['processed'] > 0 else "0%"
            ])

        stats_headers = ['Service', 'Processed', 'Updated', 'Errors', 'Update Rate']
        print(tabulate(stats_table, headers=stats_headers, tablefmt='grid'))
        print("="*60 + "\n")

    def _process_single_item(self, item: Dict[str, Any], media_type: str, service_name: str, api_client: Any, tags_id_to_label: Dict, tags_label_to_id: Dict) -> bool:
        """
        Processes a single media item. This function is designed to be run in a thread.
        Returns:
            True if the item was processed successfully, False if a permanent error occurred.
        """
        title = item["title"]

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
                    with self.changes_lock:
                        self.errors_log.append({
                            'service': service_name,
                            'title': title,
                            'reason': 'TMDB ID not found.'
                        })
                    self._log_change(service_name, title, set(), set(), success=False)
                    return False

                # Get watch providers from TMDb
                providers_response = get_providers_func(tmdb_id)

                # Handle different response types without printing debug info
                if hasattr(providers_response, 'results'):
                    results = providers_response.results
                elif isinstance(providers_response, dict):
                    results = providers_response.get('results', {})
                else:
                    results = {}

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
                    # Get readable tag names for logging
                    added_labels = {tags_id_to_label.get(t) for t in new_tags_ids - current_tags_ids if t}
                    removed_labels = {tags_id_to_label.get(t) for t in current_tags_ids - new_tags_ids if t}

                    added_labels = {label for label in added_labels if label}
                    removed_labels = {label for label in removed_labels if label}

                    item["tags"] = list(new_tags_ids)
                    # Redirect stdout only for the update call to catch the <class 'dict'> print
                    captured_output = StringIO()
                    with redirect_stdout(captured_output):
                        update_media_func(item)

                    self._log_change(service_name, title, added_labels, removed_labels, success=True)

                    change_parts = []
                    if removed_labels:
                        change_parts.append(f"Removed: {', '.join(sorted(removed_labels))}")
                    if added_labels:
                        change_parts.append(f"Added: {', '.join(sorted(added_labels))}")
                    change_summary = ". ".join(change_parts)
                    self.send_notification(f"{service_name}: {title}", change_summary)
                else:
                    self._log_change(service_name, title, set(), set(), success=True)

                return True  # Success

            except RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    # Don't log to console during progress, just wait and retry
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    # Only log after final attempt and after progress bar is done
                    with self.changes_lock:
                        self.errors_log.append({
                            'service': service_name,
                            'title': title,
                            'reason': f"Network error after {MAX_RETRIES} retries."
                        })
                    self._log_change(service_name, title, set(), set(), success=False)
                    return False
            except Exception as e:
                # Don't print exceptions during progress bar
                with self.changes_lock:
                    self.errors_log.append({
                        'service': service_name,
                        'title': title,
                        'reason': 'Unexpected error occurred.'
                    })
                self._log_change(service_name, title, set(), set(), success=False)
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
        # Disable logging during processing to keep progress bars clean
        original_level = logging.getLogger().getEffectiveLevel()
        logging.getLogger().setLevel(logging.ERROR)

        try:
            # 1. Get all media items from the service
            try:
                get_media_func = api_client.get_movie if media_type == "movie" else api_client.get_series
                all_media = get_media_func()
            except Exception as e:
                print(f"Failed to get media from {service_name}: {e}")
                return

            # 2. Ensure all required provider tags exist
            for provider in self.providers:
                api_client.create_tag(self._get_tag_label_for_provider(provider))

            # 3. Create mappings for tag IDs and labels
            all_tags = api_client.get_tag()
            tags_id_to_label = {tag["id"]: tag["label"] for tag in all_tags}
            tags_label_to_id = {tag["label"]: tag["id"] for tag in all_tags}

            # 4. Process each media item in parallel
            processed_count = 0
            total_count = len(all_media)

            with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix=f'{service_name}Worker') as executor:
                future_to_item = {
                    executor.submit(self._process_single_item, item, media_type, service_name, api_client, tags_id_to_label, tags_label_to_id): item
                    for item in all_media
                }

                # Use tqdm for a clean progress bar
                with tqdm(total=total_count, desc=f"Processing {service_name}", unit=" items",
                         bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt}") as pbar:

                    for future in as_completed(future_to_item):
                        item = future_to_item[future]
                        try:
                            if future.result():
                                processed_count += 1
                        except Exception:
                            # Silently handle exceptions during progress
                            pass
                        finally:
                            pbar.update(1)

        finally:
            # Restore original logging level
            logging.getLogger().setLevel(original_level)

        updated_count = self.service_stats[service_name]['updated']
        print(f"Finished processing {service_name}: {processed_count}/{total_count} items processed. {updated_count} items updated.")
        return processed_count

    def run(self):
        """
        The main execution method for the script with enhanced output formatting.
        """
        start_time = datetime.now()
        print("\n🎬 ELSEWHERR - Media Streaming Provider Tagger")
        print("=" * 60)

        summaries = []

        if self.config.get("radarr", {}).get("enabled"):
            radarr_config = self.config["radarr"]
            try:
                radarr_api = RadarrAPI(host_url=radarr_config["url"], api_key=radarr_config["api_key"])
                count = self._process_service("Radarr", radarr_api, "movie")
                if count is not None:
                    summaries.append(f"{count} movies")
            except Exception as e:
                print(f"Failed to initialize Radarr API client: {e}")

        if self.config.get("sonarr", {}).get("enabled"):
            sonarr_config = self.config["sonarr"]
            try:
                sonarr_api = SonarrAPI(host_url=sonarr_config["url"], api_key=sonarr_config["api_key"])
                count = self._process_service("Sonarr", sonarr_api, "series")
                if count is not None:
                    summaries.append(f"{count} series")
            except Exception as e:
                print(f"Failed to initialize Sonarr API client: {e}")

        # Print the nice summary table
        self._print_changes_summary()

        # Calculate runtime
        runtime = datetime.now() - start_time
        runtime_str = f"{runtime.total_seconds():.1f}s"

        if summaries:
            summary_message = f"Processed {' & '.join(summaries)} in {runtime_str}"
            print(f"✅ {summary_message}")
            self.send_notification("Elsewherr Run Complete", summary_message)
        else:
            print(f"❌ No services were processed successfully. Runtime: {runtime_str}")

        print("🏁 Elsewherr has finished.\n")


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