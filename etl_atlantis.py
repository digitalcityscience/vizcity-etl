from typing import Callable

import requests

from influxdb import write_points_to_influx
from parse import (
    extract_air_quality,
    extract_bike_traffic_status,
    extract_stadtrad_stations,
    extract_traffic_status,
    extract_weather_sensors,
)

BUCKET = "atlantis"


def fetch_and_transform_geoportal_events(
    url: str, extract_function: Callable, json=False
):
    print(f"Collecting remote data from {url} ...")
    request = requests.get(url)
    result_xml = request.json() if json else request.text
    print(f"Parsing events {result_xml} ...")
    events = extract_function(result_xml)
    print(events)
    events_points = [event.to_point() for event in events]
    print(
        f"Writing to timeseries db {[event.to_line_protocol() for event in events_points]} ..."
    )
    write_points_to_influx(BUCKET, events_points)


def collect_stadtrad():
    fetch_and_transform_geoportal_events(
        "https://geodienste.hamburg.de/HH_WFS_Stadtrad?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typename=de.hh.up:stadtrad_stationen",
        extract_stadtrad_stations,
    )


def collect_swis():
    fetch_and_transform_geoportal_events(
        "https://geodienste.hamburg.de/DE_HH_INSPIRE_WFS_SWIS_Sensoren?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&typename=app:swis_sensoren",
        extract_weather_sensors,
    )


def collect_air_quality():
    fetch_and_transform_geoportal_events(
        "https://geodienste.hamburg.de/HH_WFS_Luftmessnetz?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&typename=app:luftmessnetz_messwerte",
        extract_air_quality,
    )


def collect_traffic_status():
    fetch_and_transform_geoportal_events(
        "https://iot.hamburg.de/v1.1/Things?$filter=Datastreams/properties/serviceName eq 'HH_STA_AutomatisierteVerkehrsmengenerfassung'  and Datastreams/properties/layerName eq 'Anzahl_Kfz_Zaehlstelle_15-Min' &$count=true&$expand=Datastreams($filter=properties/layerName eq 'Anzahl_Kfz_Zaehlstelle_15-Min';$expand=Observations($top=1;$orderby=phenomenonTime desc))",
        extract_traffic_status,
        True,
    )

def collect_bike_traffic_status():
    fetch_and_transform_geoportal_events(
        "https://iot.hamburg.de/v1.1/Things?$filter=Datastreams/properties/serviceName eq 'HH_STA_HamburgerRadzaehlnetz' and Datastreams/properties/layerName eq 'Anzahl_Fahrraeder_Zaehlstelle_15-Min'&$count=true&$expand=Datastreams($filter=properties/layerName eq 'Anzahl_Fahrraeder_Zaehlstelle_15-Min';$expand=Observations($top=1;$orderby=phenomenonTime desc))",
        extract_bike_traffic_status,
        True,
    )


def collect():
    collect_stadtrad()
    collect_swis()
    collect_air_quality()
    collect_traffic_status()
    collect_bike_traffic_status()


collect()