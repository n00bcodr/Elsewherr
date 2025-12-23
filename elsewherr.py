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
import requests
from gotify import Gotify
from pyarr import RadarrAPI, SonarrAPI
from requests.exceptions import RequestException
from tmdbv3api import TMDb, Find, Movie, TV
from tabulate import tabulate
from tqdm import tqdm

# --- Constants ---
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 30
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
        self.config_file = args.config
        self.logger = self._setup_logging(args)

        self.config = self._load_config()
        self.tmdb = self._setup_tmdb()
        self.gotify = self._setup_gotify()
        self.discord_webhook_url = self._setup_discord()

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
        handlers = []
        
        # Only add StreamHandler if not using tqdm (we'll configure it with tqdm.write later)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        handlers.append(stream_handler)
        
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)
            
        logging.basicConfig(level=log_level, handlers=handlers, force=True)
        for logger_name in ["requests", "urllib3", "tmdbv3api", "pyarr"]:
            logging.getLogger(logger_name).setLevel(logging.WARNING)
        logger = logging.getLogger(__name__)
        if args.verbose:
            logger.debug("Verbose logging enabled.")
        return logger

    def _load_config(self) -> Dict[str, Any]:
        """
        Loads the configuration from the specified config file.
        """
        config_path = Path(self.config_file) if self.config_file else self.base_dir / "config.yaml"
        self.logger.debug(f"Loading configuration from {config_path}")
        try:
            with open(config_path, "r") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found at {config_path}. Exiting.")
            sys.exit(1)
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML configuration: {e}. Exiting.")
            sys.exit(1)

    def _setup_tmdb(self) -> TMDb:
        """Initializes and returns a configured TMDb API client."""
        tmdb_api_key = self.config.get("tmdb", {}).get("api_key")
        if not tmdb_api_key:
            self.logger.error("TMDb API key is missing from config.yaml. Exiting.")
            sys.exit(1)
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

    def _setup_discord(self) -> Optional[str]:
        """Initializes and returns the Discord webhook URL if configured."""
        if self.config.get("discord", {}).get("enabled"):
            webhook_url = self.config["discord"].get("webhook_url")
            if webhook_url:
                self.logger.debug("Discord notifications enabled.")
                return webhook_url
        self.logger.debug("Discord notifications disabled.")
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

    def send_discord_notification(self, message: str):
        """Sends a notification via Discord webhook, splitting the message smartly."""
        if self.discord_webhook_url:
            try:
                self.logger.debug("Sending Discord notification.")
                lines = message.split('\n')
                chunks = []
                current_chunk = ""
                for line in lines:
                    if len(current_chunk) + len(line) + 1 > 1990:
                        chunks.append(current_chunk)
                        current_chunk = ""
                    if not current_chunk:
                        current_chunk = line
                    else:
                        current_chunk += "\n" + line
                if current_chunk:
                    chunks.append(current_chunk)
                for chunk in chunks:
                    if chunk.strip():
                        # Use a standard code block for the tables
                        payload = {"content": f"```\n{chunk}\n```"}
                        requests.post(self.discord_webhook_url, json=payload).raise_for_status()
            except Exception as e:
                self.logger.error(f"Failed to send Discord notification: {e}")

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
        with self.changes_lock:
            self.changes_log.append({
                'service': service_name,
                'title': title[:30] + "..." if len(title) > 30 else title,
                'changes': change_summary,
            })
            if success:
                self.service_stats[service_name]['processed'] += 1
                if change_parts:
                    self.service_stats[service_name]['updated'] += 1
            else:
                self.service_stats[service_name]['errors'] += 1

    def _print_changes_summary(self):
        """
        Print neat tabular summaries and send a consolidated report to Discord.
        """
        # Use a cleaner table format
        table_format = "pretty"
        summary_output = []

        # --- Create a dynamic heading for Discord ---
        now = datetime.now()
        # Formatter for "22nd September, 2025"
        day = now.day
        if 4 <= day <= 20 or 24 <= day <= 30:
            suffix = "th"
        else:
            suffix = ["st", "nd", "rd"][day % 10 - 1]
        date_str = now.strftime(f'%B {day}{suffix}, %Y')
        summary_output.append(f"🎬 Elsewherr Summary - {date_str}\n")


        # --- 1. Changes Summary ---
        actual_changes = [change for change in self.changes_log if "No changes" not in change['changes']]
        total_processed = sum(stats['processed'] for stats in self.service_stats.values())

        if not total_processed:
            print("\nNo items were processed. Check your Radarr/Sonarr connection settings.")
            return

        if actual_changes:
            header = "📊 CHANGES SUMMARY"
            print(f"\n{header}")
            summary_output.append(header)
            table_data = [[change['service'], change['title'], change['changes']] for change in actual_changes]
            table = tabulate(table_data, headers=['Service', 'Title', 'Changes'], tablefmt=table_format, maxcolwidths=[8, 35, 55])
            print(table)
            summary_output.append(table)
        else:
            no_changes_msg = "\n📊 No tag changes"
            print(no_changes_msg)
            summary_output.append(no_changes_msg)

        # --- 2. Issues Summary ---
        if self.errors_log:
            header = "⚠️ ISSUES SUMMARY"
            print(f"\n{header}")
            summary_output.append(f"\n{header}")
            errors_table_data = [[error['service'], error['title'], error['reason']] for error in sorted(self.errors_log, key=lambda x: x['service'])]
            table = tabulate(errors_table_data, headers=['Service', 'Title', 'Reason'], tablefmt=table_format, maxcolwidths=[8, 35, 55])
            print(table)
            summary_output.append(table)

        # --- 3. Service Statistics ---
        header = "📈 STATISTICS"
        print(f"\n{header}")
        summary_output.append(f"\n{header}")
        stats_table_data = []
        for service, stats in self.service_stats.items():
            update_rate = f"{(stats['updated']/stats['processed']*100):.1f}%" if stats['processed'] > 0 else "0%"
            stats_table_data.append([service, stats['processed'], stats['updated'], stats['errors'], update_rate])
        table = tabulate(stats_table_data, headers=['Service', 'Processed', 'Updated', 'Errors', 'Update Rate'], tablefmt=table_format)
        print(table)
        summary_output.append(table)

        self.send_discord_notification("\n".join(summary_output))

    def _process_single_item(self, item: Dict[str, Any], media_type: str, service_name: str, api_client: Any, tags_id_to_label: Dict, tags_label_to_id: Dict) -> bool:
        title = item["title"]
        get_providers_func = Movie().watch_providers if media_type == "movie" else TV().watch_providers
        update_media_func = api_client.upd_movie if media_type == "movie" else api_client.upd_series
        for retry_attempt in range(MAX_RETRIES):
            try:
                tmdb_id = item.get("tmdbId") if media_type == "movie" else (Find().find_by_tvdb_id(str(item.get("tvdbId"))).get("tv_results", [{}])[0].get("id"))
                if not tmdb_id:
                    with self.changes_lock:
                        self.errors_log.append({'service': service_name, 'title': title, 'reason': 'TMDB ID not found.'})
                    self._log_change(service_name, title, set(), set(), success=False)
                    return False
                providers_response = get_providers_func(tmdb_id)
                results = getattr(providers_response, 'results', providers_response.get('results', {}))
                flatrate_providers = results.get(self.region, {}).get("flatrate", [])
                current_tags_ids = set(item.get("tags", []))
                new_tags_ids = {tag_id for tag_id in current_tags_ids if not tags_id_to_label.get(tag_id, "").startswith(self.prefix)}
                for provider in flatrate_providers:
                    if (provider_name := provider.get("provider_name")) in self.providers:
                        if (tag_id := tags_label_to_id.get(self._get_tag_label_for_provider(provider_name))):
                            new_tags_ids.add(tag_id)
                if current_tags_ids != new_tags_ids:
                    added_labels = {tags_id_to_label.get(t) for t in new_tags_ids - current_tags_ids if t}
                    removed_labels = {tags_id_to_label.get(t) for t in current_tags_ids - new_tags_ids if t}
                    item["tags"] = list(new_tags_ids)
                    with redirect_stdout(StringIO()):
                        update_media_func(item)
                    self._log_change(service_name, title, added_labels, removed_labels, success=True)
                    change_summary = ". ".join(filter(None, [f"Removed: {', '.join(sorted(removed_labels))}" if removed_labels else "", f"Added: {', '.join(sorted(added_labels))}" if added_labels else ""]))
                    self.send_notification(f"{service_name}: {title}", change_summary)
                else:
                    self._log_change(service_name, title, set(), set(), success=True)
                return True
            except RequestException as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                if retry_attempt < MAX_RETRIES - 1:
                    self.logger.warning(f"Network error for '{title}' (attempt {retry_attempt + 1}/{MAX_RETRIES}): {error_msg}. Retrying in {RETRY_DELAY_SECONDS}s...")
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    self.logger.error(f"Network error for '{title}' after {MAX_RETRIES} attempts: {error_msg}")
            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                self.logger.error(f"Unexpected error processing '{title}': {error_msg}")
                with self.changes_lock:
                    self.errors_log.append({'service': service_name, 'title': title, 'reason': f'Error: {error_msg[:50]}'})
                self._log_change(service_name, title, set(), set(), success=False)
                return False
        with self.changes_lock:
            self.errors_log.append({'service': service_name, 'title': title, 'reason': f"Network error after {MAX_RETRIES} retries."})
        self._log_change(service_name, title, set(), set(), success=False)
        return False

    def _process_service(self, service_name: str, api_client: Any, media_type: str):
        original_level = logging.getLogger().getEffectiveLevel()
        logging.getLogger().setLevel(logging.ERROR)
        try:
            get_media_func = api_client.get_movie if media_type == "movie" else api_client.get_series
            all_media = get_media_func()
            for provider in self.providers:
                api_client.create_tag(self._get_tag_label_for_provider(provider))
            all_tags = api_client.get_tag()
            tags_id_to_label = {tag["id"]: tag["label"] for tag in all_tags}
            tags_label_to_id = {tag["label"]: tag["id"] for tag in all_tags}
            
            # Configure logging to use tqdm.write during progress bar display
            class TqdmLoggingHandler(logging.Handler):
                def emit(self, record):
                    try:
                        msg = self.format(record)
                        tqdm.write(msg)
                    except Exception:
                        self.handleError(record)
            
            # Temporarily replace stream handler with tqdm-compatible one
            root_logger = logging.getLogger()
            original_handlers = root_logger.handlers[:]
            tqdm_handler = TqdmLoggingHandler()
            tqdm_handler.setFormatter(logging.Formatter("%(levelname)-8s :: %(message)s"))
            root_logger.handlers = [h for h in root_logger.handlers if not isinstance(h, logging.StreamHandler)] + [tqdm_handler]
            
            try:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [executor.submit(self._process_single_item, item, media_type, service_name, api_client, tags_id_to_label, tags_label_to_id) for item in all_media]
                    with tqdm(total=len(all_media), desc=f"Processing {service_name}", unit=" items",
                        bar_format='{l_bar}{bar} | {n_fmt}/{total_fmt}', position=0, leave=True) as pbar:
                        for future in as_completed(futures):
                            pbar.update(1)
            finally:
                # Restore original handlers
                root_logger.handlers = original_handlers
        finally:
            logging.getLogger().setLevel(original_level)
        print(f"Finished processing {service_name}: {self.service_stats[service_name]['processed']}/{len(all_media)} items processed. {self.service_stats[service_name]['updated']} items updated.")
        return self.service_stats[service_name]['processed']

    def run(self):
        start_time = datetime.now()
        print("\n🎬 ELSEWHERR - Media Streaming Provider Tagger")
        print("=" * 60)
        summaries = []
        if self.config.get("radarr", {}).get("enabled"):
            radarr_config = self.config["radarr"]
            try:
                radarr_api = RadarrAPI(host_url=radarr_config["url"], api_key=radarr_config["api_key"])
                if (count := self._process_service("Radarr", radarr_api, "movie")) is not None:
                    summaries.append(f"{count} movies")
            except Exception as e:
                print(f"Failed to initialize Radarr API client: {e}")
        if self.config.get("sonarr", {}).get("enabled"):
            sonarr_config = self.config["sonarr"]
            try:
                sonarr_api = SonarrAPI(host_url=sonarr_config["url"], api_key=sonarr_config["api_key"])
                if (count := self._process_service("Sonarr", sonarr_api, "series")) is not None:
                    summaries.append(f"{count} series")
            except Exception as e:
                print(f"Failed to initialize Sonarr API client: {e}")

        time.sleep(0.1)  # Allow tqdm to finish cleaning up

        self._print_changes_summary()
        
        # Save detailed error log if there were any errors
        if self.errors_log:
            error_log_file = self.base_dir / "logs" / f"errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            error_log_file.parent.mkdir(exist_ok=True)
            with open(error_log_file, 'w', encoding='utf-8') as f:
                f.write(f"Elsewherr Error Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")
                for error in self.errors_log:
                    f.write(f"Service: {error['service']}\n")
                    f.write(f"Title: {error['title']}\n")
                    f.write(f"Reason: {error['reason']}\n")
                    f.write("-" * 80 + "\n")
            print(f"💾 Detailed error log saved to: {error_log_file}")

        runtime = f"{(datetime.now() - start_time).total_seconds():.1f}s"
        if summaries:
            summary_message = f"Processed {' & '.join(summaries)} in {runtime}"
            print(f"✅ {summary_message}")
            self.send_notification("Elsewherr Run Complete", summary_message)
        else:
            print(f"❌ No services were processed successfully. Runtime: {runtime}")
        print("🏁 Elsewherr has finished.\n")

def main():
    parser = argparse.ArgumentParser(description="Tag Radarr/Sonarr media with streaming provider info.")
    parser.add_argument("-c", "--config", type=str, default=None, help="Path to config file (default: config.yaml)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose (DEBUG level) logging.")
    parser.add_argument("-l", "--log-to-file", action="store_true", help="Enable logging to a file.")
    args = parser.parse_args()
    app = Elsewherr(args)
    app.run()

if __name__ == "__main__":
    main()