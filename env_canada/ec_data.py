import re
import xml.etree.ElementTree as et

from bs4 import BeautifulSoup
from geopy import distance
from ratelimit import limits, RateLimitException
import requests


def ignore_ratelimit_error(fun):
    def res(*args, **kwargs):
        try:
            return fun(*args, **kwargs)
        except RateLimitException:
            return None
    return res


class ECData(object):
    SITE_LIST_URL = 'https://dd.weather.gc.ca/citypage_weather/docs/site_list_en.csv'
    AQHI_SITE_LIST_URL = 'https://dd.weather.gc.ca/air_quality/doc/AQHI_XML_File_List.xml'
    XML_URL_BASE = 'https://dd.weather.gc.ca/citypage_weather/xml/{}_{}.xml'
    AQHI_OBSERVATION_URL = 'https://dd.weather.gc.ca/air_quality/aqhi/{}/observation/realtime/xml/AQ_OBS_{}_CURRENT.xml'
    AQHI_FORECAST_URL = 'https://dd.weather.gc.ca/air_quality/aqhi/{}/forecast/realtime/xml/AQ_FCST_{}_CURRENT.xml'

    conditions_meta = {
        'temperature': {
            'xpath': './currentConditions/temperature',
            'english': 'Temperature',
            'french': 'Température'
        },
        'dewpoint': {
            'xpath': './currentConditions/dewpoint',
            'english': 'Dew Point',
            'french': 'Point de rosée'
        },
        'wind_chill': {
            'xpath': './currentConditions/windChill',
            'english': 'Wind Chill',
            'french': 'Refroidissement éolien'
        },
        'humidex': {
            'xpath': './currentConditions/humidex',
            'english': 'Humidex',
            'french': 'Humidex'
        },
        'pressure': {
            'xpath': './currentConditions/pressure',
            'english': 'Pressure',
            'french': 'Pression'
        },
        'tendency': {
            'xpath': './currentConditions/pressure',
            'attribute': 'tendency',
            'english': 'Tendency',
            'french': 'Tendance'
        },
        'humidity': {
            'xpath': './currentConditions/relativeHumidity',
            'english': 'Humidity',
            'french': 'Humidité'
        },
        'visibility': {
            'xpath': './currentConditions/visibility',
            'english': 'Visibility',
            'french': 'Visibilité'
        },
        'condition': {
            'xpath': './currentConditions/condition',
            'english': 'Condition',
            'french': 'Condition'
        },
        'wind_speed': {
            'xpath': './currentConditions/wind/speed',
            'english': 'Wind Speed',
            'french': 'Vitesse de vent'
        },
        'wind_gust': {
            'xpath': './currentConditions/wind/gust',
            'english': 'Wind Gust',
            'french': 'Rafale de vent'
        },
        'wind_dir': {
            'xpath': './currentConditions/wind/direction',
            'english': 'Wind Direction',
            'french': 'Direction de vent'
        },
        'wind_bearing': {
            'xpath': './currentConditions/wind/bearing',
            'english': 'Wind Bearing',
            'french': 'Palier de vent'
        },
        'high_temp': {
            'xpath': './forecastGroup/forecast/temperatures/temperature[@class="high"]',
            'english': 'High Temperature',
            'french': 'Haute température'
        },
        'low_temp': {
            'xpath': './forecastGroup/forecast/temperatures/temperature[@class="low"]',
            'english': 'Low Temperature',
            'french': 'Basse température'
        },
        'uv_index': {
            'xpath': './forecastGroup/forecast/uv/index',
            'english': 'UV Index',
            'french': 'Indice UV'
        },
        'pop': {
            'xpath': './forecastGroup/forecast/abbreviatedForecast/pop',
            'english': 'Chance of Precip.',
            'french': 'Probabilité d\'averses'
        },
        'icon_code': {
            'xpath': './currentConditions/iconCode',
            'english': 'Icon Code',
            'french': 'Code icône'
        },
        'precip_yesterday': {
            'xpath': './yesterdayConditions/precip',
            'english': 'Precipitation Yesterday',
            'french': 'Précipitation d\'hier'
        },
    }

    aqhi_meta = {
        'label': {
            'english': 'Air Quality Health Index',
            'french': 'Cote air santé'
        }
    }

    summary_meta = {
        'forecast_period': {
            'xpath': './forecastGroup/forecast/period',
            'attribute': 'textForecastName',
        },
        'text_summary': {
            'xpath': './forecastGroup/forecast/textSummary',
        },
        'label': {
            'english': 'Forecast',
            'french': 'Prévision'
        }
    }

    alerts_meta = {
        'warnings': {
            'english': {
                'label': 'Warnings',
                'pattern': '.*WARNING((?!ENDED).)*$'
            },
            'french': {
                'label': 'Alertes',
                'pattern': '.*(ALERTE|AVERTISSEMENT)((?!TERMINÉ).)*$'
            }
        },
        'watches': {
            'english': {
                'label': 'Watches',
                'pattern': '.*WATCH((?!ENDED).)*$'
            },
            'french': {
                'label': 'Veilles',
                'pattern': '.*VEILLE((?!TERMINÉ).)*$'
            }
        },
        'advisories': {
            'english': {
                'label': 'Advisories',
                'pattern': '.*ADVISORY((?!ENDED).)*$'
            },
            'french': {
                'label': 'Avis',
                'pattern': '.*AVIS((?!TERMINÉ).)*$'
            }
        },
        'statements': {
            'english': {
                'label': 'Statements',
                'pattern': '.*STATEMENT((?!ENDED).)*$'
            },
            'french': {
                'label': 'Bulletins',
                'pattern': '.*BULLETIN((?!TERMINÉ).)*$'
            }
        },
        'endings': {
            'english': {
                'label': 'Endings',
                'pattern': '.*ENDED'
            },
            'french': {
                'label': 'Terminaisons',
                'pattern': '.*TERMINÉE?'
            }
        }
    }

    metadata_meta = {
        'timestamp': {
            'xpath': './currentConditions/dateTime/timeStamp',
        },
        'location': {
            'xpath': './location/name',
        },
        'station': {
            'xpath': './currentConditions/station',
        },
    }

    """Get data from Environment Canada."""

    def __init__(self,
                 station_id=None,
                 aqhi_id=None,
                 coordinates=None,
                 language='english'):
        """Initialize the data object."""
        self.language = language
        self.language_abr = language[:2].upper()
        self.zone_name_tag = 'name_%s_CA' % self.language_abr.lower()
        self.region_name_tag = 'name%s' % self.language_abr.title()

        self.metadata = {}
        self.conditions = {}
        self.alerts = {}
        self.daily_forecasts = []
        self.hourly_forecasts = []
        self.aqhi = {}
        self.forecast_time = ''

        if station_id:
            self.station_id = station_id
        else:
            self.station_id = self.closest_site(coordinates[0],
                                                coordinates[1])
        if aqhi_id:
            self.aqhi_id = (aqhi_id.split('/'))
        else:
            self.aqhi_id = self.closest_aqhi(coordinates[0],
                                             coordinates[1])

        self.update()

    @ignore_ratelimit_error
    @limits(calls=2, period=60)
    def update(self):
        """Get the latest data from Environment Canada."""
        result = requests.get(self.XML_URL_BASE.format(self.station_id,
                                                       self.language[0]),
                              timeout=10)
        site_xml = result.content.decode('iso-8859-1')
        xml_object = et.fromstring(site_xml)

        # Update metadata
        for m, meta in self.metadata_meta.items():
            self.metadata[m] = xml_object.find(meta['xpath']).text

        # Update current conditions
        def get_condition(meta):
            condition = {}

            element = xml_object.find(meta['xpath'])

            if element is not None:
                if meta.get('attribute'):
                    condition['value'] = element.attrib.get(meta['attribute'])
                else:
                    condition['value'] = element.text
                    if element.attrib.get('units'):
                        condition['unit'] = element.attrib.get('units')
            return condition

        for c, meta in self.conditions_meta.items():
            self.conditions[c] = {'label': meta[self.language]}
            self.conditions[c].update(get_condition(meta))

        # Update text summary
        period = get_condition(self.summary_meta['forecast_period'])['value']
        summary = get_condition(self.summary_meta['text_summary'])['value']

        self.conditions['text_summary'] = {
            'label': self.summary_meta['label'][self.language],
            'value': '. '.join([period, summary])
        }

        # Update alerts
        for category, meta in self.alerts_meta.items():
            self.alerts[category] = {'value': [],
                                     'label': meta[self.language]['label']}

        alert_elements = xml_object.findall('./warnings/event')
        alert_list = [e.attrib.get('description').strip() for e in alert_elements]

        if alert_list:
            alert_url = xml_object.find('./warnings').attrib.get('url')
            alert_html = requests.get(url=alert_url).content
            alert_soup = BeautifulSoup(alert_html, 'html.parser')

            for title in alert_list:
                for category, meta in self.alerts_meta.items():
                    category_match = re.search(meta[self.language]['pattern'], title)
                    if category_match:

                        alert = {'title': title.title(),
                                 'date': '',
                                 'detail': ''}

                        html_title = ''

                        for s in alert_soup('strong'):
                            if re.sub('terminé', 'est terminé', title.lower()) in s.text.lower():
                                html_title = s.text

                        date_pattern = 'p:contains("{}") span'
                        date_match = alert_soup.select(date_pattern.format(html_title))
                        if date_match:
                            alert.update({'date': date_match[0].text})

                        if category != 'endings':
                            detail_pattern = 'p:contains("{}") ~ p'
                            detail_match = alert_soup.select(detail_pattern.format(html_title))
                            if detail_match:
                                detail = re.sub(r'\.(?=\S)', '. ', detail_match[0].text)
                                alert.update({'detail': detail})

                        self.alerts[category]['value'].append(alert)

        # Update daily forecasts
        self.forecast_time = xml_object.findtext('./forecastGroup/dateTime/timeStamp')
        self.daily_forecasts = []
        self.hourly_forecasts = []

        for f in xml_object.findall('./forecastGroup/forecast'):
            self.daily_forecasts.append({
                'period': f.findtext('period'),
                'text_summary': f.findtext('textSummary'),
                'icon_code': f.findtext('./abbreviatedForecast/iconCode'),
                'temperature': f.findtext('./temperatures/temperature'),
                'temperature_class': f.find('./temperatures/temperature').attrib.get('class')
            })

        # Update hourly forecasts
        for f in xml_object.findall('./hourlyForecastGroup/hourlyForecast'):
            self.hourly_forecasts.append({
                'period': f.attrib.get('dateTimeUTC'),
                'condition': f.findtext('./condition'),
                'temperature': f.findtext('./temperature'),
                'icon_code': f.findtext('./iconCode'),
                'precip_probability': f.findtext('./lop'),
            })

        # Update AQHI current condition
        result = requests.get(self.AQHI_OBSERVATION_URL.format(self.aqhi_id[0],
                                                               self.aqhi_id[1]),
                              timeout=10)
        site_xml = result.content.decode("utf-8")
        xml_object = et.fromstring(site_xml)

        element = xml_object.find('airQualityHealthIndex')
        if element is not None:
            self.aqhi['current'] = element.text
        else:
            self.aqhi['current'] = None

        self.conditions['air_quality'] = {
            'label': self.aqhi_meta['label'][self.language],
            'value': self.aqhi['current']
        }

        element = xml_object.find('./dateStamp/UTCStamp')
        if element is not None:
            self.aqhi['utc_time'] = element.text
        else:
            self.aqhi['utc_time'] = None

        # Update AQHI forecasts
        result = requests.get(self.AQHI_FORECAST_URL.format(self.aqhi_id[0],
                                                            self.aqhi_id[1]),
                              timeout=10)
        site_xml = result.content.decode("ISO-8859-1")
        xml_object = et.fromstring(site_xml)

        self.aqhi['forecasts'] = {'daily': [],
                                  'hourly': []}

        # Update daily forecasts
        period = None
        for f in xml_object.findall("./forecastGroup/forecast"):
            for p in f.findall("./period"):
                if self.language_abr == p.attrib["lang"]:
                    period = p.attrib["forecastName"]
            self.aqhi['forecasts']['daily'].append(
                {
                    "period": period,
                    "aqhi": f.findtext("./airQualityHealthIndex"),
                }
            )

        # Update hourly forecasts
        for f in xml_object.findall("./hourlyForecastGroup/hourlyForecast"):
            self.aqhi['forecasts']['hourly'].append(
                {"period": f.attrib["UTCTime"], "aqhi": f.text}
            )

    def get_ec_sites(self):
        """Get list of all sites from Environment Canada, for auto-config."""
        import csv
        import io

        sites = []

        sites_csv_string = requests.get(self.SITE_LIST_URL, timeout=10).text
        sites_csv_stream = io.StringIO(sites_csv_string)

        sites_csv_stream.seek(0)
        next(sites_csv_stream)

        sites_reader = csv.DictReader(sites_csv_stream)

        for site in sites_reader:
            if site['Province Codes'] != 'HEF':
                site['Latitude'] = float(site['Latitude'].replace('N', ''))
                site['Longitude'] = -1 * float(site['Longitude'].replace('W', ''))
                sites.append(site)

        return sites

    def closest_site(self, lat, lon):
        """Return the province/site_code of the closest station to our lat/lon."""
        site_list = self.get_ec_sites()

        def site_distance(site):
            """Calculate distance to a site."""
            return distance.distance((lat, lon), (site['Latitude'], site['Longitude']))

        closest = min(site_list, key=site_distance)

        return '{}/{}'.format(closest['Province Codes'], closest['Codes'])

    def get_aqhi_regions(self):
        """Get list of all AQHI regions from Environment Canada, for auto-config."""
        result = requests.get(self.AQHI_SITE_LIST_URL, timeout=10)
        site_xml = result.content.decode("utf-8")
        xml_object = et.fromstring(site_xml)

        regions = []
        for zone in xml_object.findall("./EC_administrativeZone"):
            _zone_attribs = zone.attrib
            _zone_attrib = {
                "abbreviation": _zone_attribs["abreviation"],
                "zone_name": _zone_attribs[self.zone_name_tag],
            }
            for region in zone.findall("./regionList/region"):
                _region_attribs = region.attrib

                _region_attrib = {"region_name": _region_attribs[self.region_name_tag],
                                  "cgndb": _region_attribs["cgndb"],
                                  "latitude": float(_region_attribs["latitude"]),
                                  "longitude": float(_region_attribs["longitude"])}
                _children = region.getchildren()
                for child in _children:
                    _region_attrib[child.tag] = child.text
                _region_attrib.update(_zone_attrib)
                regions.append(_region_attrib)
        return regions

    def closest_aqhi(self, lat, lon):
        """Return the AQHI region and site ID of the closest site."""
        region_list = self.get_aqhi_regions()

        def site_distance(site):
            """Calculate distance to a region."""
            return distance.distance(
                (lat, lon), (site["latitude"], site["longitude"])
            )
        closest = min(region_list, key=site_distance)

        return closest['abbreviation'], closest['cgndb']
