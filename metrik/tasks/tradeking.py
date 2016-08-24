import requests
from requests_oauthlib import OAuth1Session
from luigi.parameter import DateParameter, Parameter
from datetime import timedelta
from dateutil.parser import parse
import logging

from metrik.tasks.base import MongoCreateTask, MongoRateLimit
from metrik.conf import get_config


class TradekingApi(object):

    format_json = '.json'
    format_xml = '.xml'
    root_url = 'https://api.tradeking.com/v1/'

    def __init__(self):
        config = get_config()
        self.session = OAuth1Session(
            client_key=config.get('tradeking', 'consumer_key'),
            client_secret=config.get('tradeking', 'consumer_secret'),
            resource_owner_key=config.get('tradeking', 'oauth_token'),
            resource_owner_secret=config.get('tradeking', 'oauth_token_secret')
        )

    def api_request(self, url, params=None, format=format_json, **kwargs):
        full_url = self.root_url + url + format
        return self.session.get(full_url, params=params, **kwargs)


class Tradeking1mTimesales(MongoCreateTask):
    # While this is marked as an idempotent task, it only goes back about
    # a week or so. Be careful.
    present = DateParameter()
    symbol = Parameter()

    def acquire_lock(self, service, limit, interval, max_tries=5, backoff=.5):
        return super(Tradeking1mTimesales, self).acquire_lock(
            'tradeking', 60, timedelta(minutes=1)
        )

    @staticmethod
    def retrieve_data(present, symbol):
        ratelimit = MongoRateLimit()
        tradeking = TradekingApi()
        did_acquire = ratelimit.acquire_lock(
            service='tradeking',
            limit=60,
            interval=timedelta(minutes=1)
        )
        if did_acquire:
            json_data = tradeking.api_request('market/timesales', {
                'symbols': symbol,
                'interval': '1min',
                'startdate': present.strftime('%Y-%m-%d'),
                'enddate': present.strftime('%Y-%m-%d')
            }).json()

            quotes = json_data['response']['quotes']['quote']
            def format_quote(quote):
                return {
                    'last': float(quote['last']),
                    'lo': float(quote['lo']),
                    'vl': int(quote['vl']),
                    'datetime': parse(quote['datetime']),
                    'incr_vl': int(quote['incr_vl']),
                    'hi': float(quote['hi']),
                    'timestamp': parse(quote['timestamp']),
                    'date': parse(quote['date']).date(),
                    'opn': float(quote['opn'])
                }
            quotes_typed = [format_quote(q) for q in quotes]

            return {
                'symbol': symbol,
                'quotes': quotes_typed
            }
        else:
            logging.error('Unable to acquire lock for Tradeking ticker {}'
                          .format(symbol))
            return {}
