from typing import Callable

import requests

from influxdb import write_points_to_influx
from parse import extract_stadtrad_stations, extract_weather_sensors

BUCKET = "atlantis"


def fetch_and_transform_geoportal_xml_events(url: str, extract_function: Callable):
    print(f"Collecting remote data from {url} ...")
    request = requests.get(url)
    result_xml = request.text
    print(f"Parsing events {result_xml} ...")
    events = extract_function(result_xml)
    print(events)
    events_points = [event.to_point() for event in events]
    print(f"Writing to timeseries db {events_points} ...")
    write_points_to_influx(BUCKET, events_points)


def collect_stadtrad():
    fetch_and_transform_geoportal_xml_events(
        "https://geodienste.hamburg.de/HH_WFS_Stadtrad?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typename=de.hh.up:stadtrad_stationen",
        extract_stadtrad_stations,
    )


def collect_swis():
    fetch_and_transform_geoportal_xml_events(
        "https://geodienste.hamburg.de/DE_HH_INSPIRE_WFS_SWIS_Sensoren?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&typename=app:swis_sensoren",
        extract_weather_sensors,
    )


def collect():
    collect_stadtrad()
    collect_swis()

