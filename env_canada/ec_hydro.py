import csv
import io
import random

from dateutil.parser import isoparse
from kombu import Connection, Consumer, Exchange, Queue
from geopy import distance
import requests
import socket

SITE_LIST_URL = 'https://dd.weather.gc.ca/hydrometric/doc/hydrometric_StationList.csv'
READINGS_URL = 'https://dd.weather.gc.ca/hydrometric/csv/{prov}/hourly/{prov}_{station}_hourly_hydrometric.csv'
AMQP_URL = 'amqps://anonymous:anonymous@dd.weather.gc.ca/'
AMQP_EXCHANGE = 'xpublic'
HYDRO_QUEUE_NAME = 'q_anonymous_env-canada-pypi_' + str(random.randrange(100000))
HYDRO_ROUTING_KEY = 'v02.post.hydrometric.csv.{prov}.hourly.#'
HYDRO_MESSAGE = '/hydrometric/csv/{prov}/hourly/{prov}_{station}_hourly_hydrometric.csv'


class ECHydro(object):

    """Get hydrometric data from Environment Canada."""

    def __init__(self,
                 province=None,
                 station=None,
                 coordinates=None):
        """Initialize the data object."""
        self.measurements = {}
        self.timestamp = None
        self.location = None

        if province and station:
            self.province = province
            self.station = station
        else:
            closest = self.closest_site(coordinates[0], coordinates[1])
            self.province = closest['Prov']
            self.station = closest['ID']
            self.location = closest['Name'].title()

        self.fetch_new_data()

        """Setup AMQP"""
        self.connection = Connection(AMQP_URL)
        self.queue = Queue(name=HYDRO_QUEUE_NAME,
                           exchange=Exchange(AMQP_EXCHANGE, no_declare=True),
                           routing_key=HYDRO_ROUTING_KEY.format(prov=self.province))

    def update(self):
        try:
            with Consumer(channel=self.connection, queues=self.queue, callbacks=[self.process_message]):
                self.connection.drain_events(timeout=5)
        except socket.timeout:
            pass

    def process_message(self, body, message):
        if HYDRO_MESSAGE.format(prov=self.province, station=self.station) in body:
            self.fetch_new_data()

    def fetch_new_data(self):
        """Get the latest data from Environment Canada."""
        hydro_csv_response = requests.get(READINGS_URL.format(prov=self.province,
                                                              station=self.station),
                                          timeout=10)
        hydro_csv_string = hydro_csv_response.content.decode('utf-8-sig')
        hydro_csv_stream = io.StringIO(hydro_csv_string)

        header = [h.split('/')[0].strip() for h in hydro_csv_stream.readline().split(',')]
        readings_reader = csv.DictReader(hydro_csv_stream, fieldnames=header)

        readings = [r for r in readings_reader]
        if len(readings) > 0:
            latest = readings[-1]

            if latest['Water Level'] != '':
                self.measurements['water_level'] = {
                    'label': 'Water Level',
                    'value': float(latest['Water Level']),
                    'unit': 'm'
                }

            if latest['Discharge'] != '':
                self.measurements['discharge'] = {
                    'label': 'Discharge',
                    'value': float(latest['Discharge']),
                    'unit': 'm³/s'
                }

            self.timestamp = isoparse(readings[-1]['Date'])

    @staticmethod
    def get_hydro_sites():

        """Get list of all sites from Environment Canada, for auto-config."""

        sites = []

        sites_csv_bytes = requests.get(SITE_LIST_URL, timeout=10).content
        sites_csv_string = sites_csv_bytes.decode('utf-8-sig')
        sites_csv_stream = io.StringIO(sites_csv_string)

        header = [h.split('/')[0].strip() for h in sites_csv_stream.readline().split(',')]
        sites_reader = csv.DictReader(sites_csv_stream, fieldnames=header)

        for site in sites_reader:
            site['Latitude'] = float(site['Latitude'])
            site['Longitude'] = float(site['Longitude'])
            sites.append(site)

        return sites

    def closest_site(self, lat, lon):
        """Return the province/site_code of the closest station to our lat/lon."""
        site_list = self.get_hydro_sites()

        def site_distance(site):
            """Calculate distance to a site."""
            return distance.distance((lat, lon),
                                     (site['Latitude'], site['Longitude']))

        closest = min(site_list, key=site_distance)

        return closest
