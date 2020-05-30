import random
import socket
import xml.etree.ElementTree as et

from geopy import distance
from kombu import Connection, Consumer, Exchange, Queue
import requests

AQHI_SITE_LIST_URL = "https://dd.weather.gc.ca/air_quality/doc/AQHI_XML_File_List.xml"
AQHI_OBSERVATION_URL = "https://dd.weather.gc.ca/air_quality/aqhi/{}/observation/realtime/xml/AQ_OBS_{}_CURRENT.xml"
AQHI_FORECAST_URL = "https://dd.weather.gc.ca/air_quality/aqhi/{}/forecast/realtime/xml/AQ_FCST_{}_CURRENT.xml"

AMQP_URL = "amqps://anonymous:anonymous@dd.weather.gc.ca/"
AMQP_EXCHANGE = "xpublic"
AQHI_QUEUE_NAME = "q_anonymous_env-canada-pypi_" + str(random.randrange(100000))
AQHI_ROUTING_KEY = "v02.post.air_quality.aqhi.#"
OBSERVATION_MESSAGE = (
    "/air_quality/aqhi/{}/observation/realtime/xml/AQ_OBS_{}_CURRENT.xml"
)
FORECAST_MESSAGE = "/air_quality/aqhi/{}/forecast/realtime/xml/AQ_FCST_{}_CURRENT.xml"


class ECAQHI(object):

    """Get air quality data from Environment Canada."""

    def __init__(self, aqhi_id=None, coordinates=None, language="english"):
        """Initialize the data object."""
        self.language = language
        self.language_abr = language[:2].upper()

        self.aqhi = {}
        self.forecast_time = ""

        if aqhi_id is None:
            self.aqhi_id = self.closest_aqhi(
                float(coordinates[0]), float(coordinates[1])
            )
        else:
            zone = str.lower(aqhi_id.split("/")[0])
            region = str.upper(aqhi_id.split("/")[1])
            self.aqhi_id = zone, region

        """Initialize data"""
        self.fetch_new_observation()
        self.fetch_new_forecast()

        """Setup AMQP"""
        self.connection = Connection(AMQP_URL)
        self.queue = Queue(
            name=AQHI_QUEUE_NAME,
            exchange=Exchange(AMQP_EXCHANGE, no_declare=True),
            routing_key=AQHI_ROUTING_KEY,
        )

    def update(self):
        try:
            with Consumer(
                channel=self.connection,
                queues=self.queue,
                callbacks=[self.process_message],
            ):
                self.connection.drain_events(timeout=5)
        except socket.timeout:
            pass

    def process_message(self, body, message):
        if OBSERVATION_MESSAGE.format(self.aqhi_id[0], self.aqhi_id[1]) in body:
            self.fetch_new_observation()
        elif FORECAST_MESSAGE.format(self.aqhi_id[0], self.aqhi_id[1]) in body:
            self.fetch_new_forecast()

    def fetch_new_observation(self):
        # Update AQHI current condition
        aqhi_result = requests.get(
            AQHI_OBSERVATION_URL.format(self.aqhi_id[0], self.aqhi_id[1]), timeout=10
        )
        aqhi_xml = aqhi_result.content.decode("utf-8")
        aqhi_tree = et.fromstring(aqhi_xml)

        element = aqhi_tree.find("airQualityHealthIndex")
        if element is not None:
            self.aqhi["current"] = element.text
        else:
            self.aqhi["current"] = None

        element = aqhi_tree.find("./dateStamp/UTCStamp")
        if element is not None:
            self.aqhi["utc_time"] = element.text
        else:
            self.aqhi["utc_time"] = None

    def fetch_new_forecast(self):
        # Update AQHI forecasts
        aqhi_result = requests.get(
            AQHI_FORECAST_URL.format(self.aqhi_id[0], self.aqhi_id[1]), timeout=10
        )
        aqhi_xml = aqhi_result.content.decode("ISO-8859-1")
        aqhi_tree = et.fromstring(aqhi_xml)

        self.aqhi["forecasts"] = {"daily": [], "hourly": []}

        # Update AQHI daily forecasts
        period = None
        for f in aqhi_tree.findall("./forecastGroup/forecast"):
            for p in f.findall("./period"):
                if self.language_abr == p.attrib["lang"]:
                    period = p.attrib["forecastName"]
            self.aqhi["forecasts"]["daily"].append(
                {"period": period, "aqhi": f.findtext("./airQualityHealthIndex")}
            )

        # Update AQHI hourly forecasts
        for f in aqhi_tree.findall("./hourlyForecastGroup/hourlyForecast"):
            self.aqhi["forecasts"]["hourly"].append(
                {"period": f.attrib["UTCTime"], "aqhi": f.text}
            )

    def get_aqhi_regions(self):
        """Get list of all AQHI regions from Environment Canada, for auto-config."""
        zone_name_tag = "name_%s_CA" % self.language_abr.lower()
        region_name_tag = "name%s" % self.language_abr.title()

        result = requests.get(AQHI_SITE_LIST_URL, timeout=10)
        site_xml = result.content.decode("utf-8")
        xml_object = et.fromstring(site_xml)

        regions = []
        for zone in xml_object.findall("./EC_administrativeZone"):
            _zone_attribs = zone.attrib
            _zone_attrib = {
                "abbreviation": _zone_attribs["abreviation"],
                "zone_name": _zone_attribs[zone_name_tag],
            }
            for region in zone.findall("./regionList/region"):
                _region_attribs = region.attrib

                _region_attrib = {
                    "region_name": _region_attribs[region_name_tag],
                    "cgndb": _region_attribs["cgndb"],
                    "latitude": float(_region_attribs["latitude"]),
                    "longitude": float(_region_attribs["longitude"]),
                }
                for child in list(region):
                    _region_attrib[child.tag] = child.text
                _region_attrib.update(_zone_attrib)
                regions.append(_region_attrib)
        return regions

    def closest_aqhi(self, lat, lon):
        """Return the AQHI region and site ID of the closest site."""
        region_list = self.get_aqhi_regions()

        def site_distance(site):
            """Calculate distance to a region."""
            return distance.distance((lat, lon), (site["latitude"], site["longitude"]))

        closest = min(region_list, key=site_distance)

        return closest["abbreviation"], closest["cgndb"]
