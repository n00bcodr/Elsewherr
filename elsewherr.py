import argparse
from gotify import Gotify
import logging
import os
import re
from pyarr import SonarrAPI, RadarrAPI
from tmdbv3api import TMDb, Find, Movie, TV
import yaml

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', help='Set log level to debug.')
    parser.add_argument('-l', '--log-to-file', action='store_true', help='Enable logging to file. (logs\elsewherr.log)')

    return parser.parse_args()

def setup(args):
    global config
    global gotify
    global logger

    dir = os.path.dirname(os.path.abspath(__file__))
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s :: %(levelname)s :: %(message)s',
        handlers=list(filter(None, [
            logging.FileHandler(filename=os.path.join(dir, 'logs', 'elsewherr.log')) if args.log_to_file else None,
            logging.StreamHandler()
        ]))
    )

    logger = logging.getLogger('elsewherr')
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    logger.info('Elsewherr starting.')
    logger.debug('DEBUG Logging Enabled')
    logger.debug('Loading Config and setting the list of required Providers')
    config = yaml.safe_load(open(os.path.join(dir, 'config.yaml')))
    logger.debug(config)

    tmdb = TMDb()
    tmdb.api_key = config['tmdb']['api_key']

    gotify = None
    if config.get('gotify') and config['gotify'].get('enabled'):
        gotify = Gotify(base_url=config['gotify']['url'], app_token=config['gotify']['token'])

def get_tag_label_for_provider(provider_name):
    return f"{config['prefix']}{re.sub('[^A-Za-z0-9]+', '', provider_name)}".lower()

def process_radarr():
    radarr = RadarrAPI(host_url=config['radarr']['url'], api_key=config['radarr']['api_key'])
    movies = radarr.get_movie()

    for provider in config['providers']:
        response = radarr.create_tag(get_tag_label_for_provider(provider))
        logger.debug('Response: %s' % response)

    all_tags = radarr.get_tag()
    tags_id_to_label = dict((tag['id'], tag['label']) for tag in all_tags)
    tags_label_to_id = dict((tag['label'], tag['id']) for tag in all_tags)

    for movie in movies:
        try:
            logger.debug('--------------------------------------------------')
            logger.debug('Movie: %s' % movie['title'])
            logger.debug(f"Existing Tags: {', '.join(map(lambda x: tags_id_to_label.get(x), movie['tags'])) if len(movie['tags']) > 0 else 'None'}")
            tags_list = list(filter(lambda x: not tags_id_to_label.get(x).startswith(config['prefix'].lower()), movie['tags']))

            region_code = config['tmdb']['region']
            watch_providers_obj = Movie().watch_providers(movie['tmdbId'])

            watch_providers_dict = watch_providers_obj.__dict__

            providers = watch_providers_dict.get('results', {}).get(region_code, {}).get('flatrate', [])

            if not providers:
                logger.debug(f"No flatrate providers found for {movie['title']} in region '{region_code}'.")

            for provider_dict in providers:
                provider_name = provider_dict.get('provider_name')
                if provider_name and provider_name in config['providers']:
                    logger.debug('Adding provider: %s' % provider_name)
                    tags_list.append(tags_label_to_id.get(get_tag_label_for_provider(provider_name)))
                else:
                    logger.debug('Skipping provider: %s' % provider_name)

            logger.debug(f"Resultant Tags: {', '.join(map(lambda x: tags_id_to_label.get(x), tags_list)) if len(tags_list) > 0 else 'None'}")

            if set(movie['tags']) != set(tags_list):
                removed_tags = list(map(lambda x: tags_id_to_label.get(x), set(movie['tags']) - set(tags_list)))
                added_tags = list(map(lambda x: tags_id_to_label.get(x), set(tags_list) - set(movie['tags'])))

                message = f"{'Removed tags: ' + ','.join(removed_tags) + ' ' if len(removed_tags) > 0 else ''}"
                message += f"{'Added tags: ' + ','.join(added_tags) if len(added_tags) > 0 else ''}"
                send_notification(f"{movie['title']}", message)

                movie['tags'] = tags_list
                radarr.upd_movie(movie)
        except Exception as e:
            logger.error(e, exc_info=True)
            logger.error('Failed to process movie %s' % movie['title'])
            continue

    logger.debug('--------------------------------------------------')
    logger.info('Processed %i movies.' % len(movies))
    return len(movies)

def process_sonarr():
    sonarr = SonarrAPI(host_url=config['sonarr']['url'], api_key=config['sonarr']['api_key'])
    all_series = sonarr.get_series()

    for provider in config['providers']:
        response = sonarr.create_tag(get_tag_label_for_provider(provider))
        logger.debug('Response: %s' % response)

    all_tags = sonarr.get_tag()
    tags_id_to_label = dict((tag['id'], tag['label']) for tag in all_tags)
    tags_label_to_id = dict((tag['label'], tag['id']) for tag in all_tags)

    for series in all_series:
        try:
            logger.debug('--------------------------------------------------')
            logger.debug('Series: %s' % series['title'])
            logger.debug(f"Existing Tags: {', '.join(map(lambda x: tags_id_to_label.get(x), series['tags'])) if len(series['tags']) > 0 else 'None'}")

            result = Find().find_by_tvdb_id(str(series['tvdbId']))
            tmdb_id = result['tv_results'][0]['id']

            tags_list = list(filter(lambda x: not tags_id_to_label.get(x).startswith(config['prefix'].lower()), series['tags']))

            region_code = config['tmdb']['region']
            watch_providers_obj = TV().watch_providers(tmdb_id)

            watch_providers_dict = watch_providers_obj.__dict__

            providers = watch_providers_dict.get('results', {}).get(region_code, {}).get('flatrate', [])

            if not providers:
                logger.debug(f"No flatrate providers found for {series['title']} in region '{region_code}'.")

            for provider_dict in providers:
                provider_name = provider_dict.get('provider_name')
                if provider_name and provider_name in config['providers']:
                    logger.debug('Adding provider: %s' % provider_name)
                    tags_list.append(tags_label_to_id.get(get_tag_label_for_provider(provider_name)))
                else:
                    logger.debug('Skipping provider: %s' % provider_name)

            logger.debug(f"Resultant Tags: {', '.join(map(lambda x: tags_id_to_label.get(x), tags_list)) if len(tags_list) > 0 else 'None'}")

            if set(series['tags']) != set(tags_list):
                removed_tags = list(map(lambda x: tags_id_to_label.get(x), set(series['tags']) - set(tags_list)))
                added_tags = list(map(lambda x: tags_id_to_label.get(x), set(tags_list) - set(series['tags'])))

                message = f"{'Removed tags: ' + ','.join(removed_tags) + ' ' if len(removed_tags) > 0 else ''}"
                message += f"{'Added tags: ' + ','.join(added_tags) if len(added_tags) > 0 else ''}"
                send_notification(f"{series['title']}", message)

                series['tags'] = tags_list
                sonarr.upd_series(series)

        except Exception as e:
            logger.error(e, exc_info=True)
            logger.error('Failed to process series %s' % series['title'])
            continue

    logger.debug('--------------------------------------------------')
    logger.info('Processed %i series.' % len(all_series))
    return len(all_series)

def send_notification(title, message):
    if gotify:
        gotify.create_message(message, title=title, priority=1)

def execute():
    setup(get_args())
    summaries = []

    if config['radarr']['enabled']:
        count = process_radarr()
        summaries.append(f"{count} movies")

    if config['sonarr']['enabled']:
        count = process_sonarr()
        summaries.append(f"{count} series")

    if len(summaries) > 0:
        send_notification('Execution Completed', f"Processed {' & '.join(summaries)}")

    logger.info('Elsewherr completed.')

if __name__ == '__main__':
    execute()