import os
from typing import Callable

import requests

from influxdb import write_points_to_influx
from models import AirportArrival
from parse import (
    extract_air_quality,
    extract_ev_charging_events,
    extract_stadtrad_stations,
    extract_weather_sensors,
    parse_airport_arrivals,
)

AIRPORT_API_KEY = os.getenv("AIRPORT_API_KEY", "NO_KEY_PROVIDED")


def fetch_and_transform_geoportal_events(
    bucket: str, url: str, extract_function: Callable, json=False
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
    write_points_to_influx(bucket, events_points)


def collect_stadtrad(bucket: str):
    fetch_and_transform_geoportal_events(
        bucket,
        "https://geodienste.hamburg.de/HH_WFS_Stadtrad?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typename=de.hh.up:stadtrad_stationen",
        extract_stadtrad_stations,
    )


def collect_air_quality(bucket: str):
    fetch_and_transform_geoportal_events(
        bucket,
        "https://geodienste.hamburg.de/HH_WFS_Luftmessnetz?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&typename=app:luftmessnetz_messwerte",
        extract_air_quality,
    )


def collect_swis(bucket: str):
    fetch_and_transform_geoportal_events(
        bucket,
        "https://geodienste.hamburg.de/DE_HH_INSPIRE_WFS_SWIS_Sensoren?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&typename=app:swis_sensoren",
        extract_weather_sensors,
    )


def collect_e_charging_stations(bucket: str):
    fetch_and_transform_geoportal_events(
        bucket,
        "https://iot.hamburg.de/v1.0/Things?$filter=Datastreams/properties/serviceName eq 'HH_STA_E-Ladestationen'&$count=true&$expand=Locations,Datastreams($expand=Observations($top=1;$orderby=phenomenonTime desc),Sensor,ObservedProperty)",
        extract_ev_charging_events,
        True,
    )


def collect_airport_arrivals(bucket: str):
    url = "https://rest.api.hamburg-airport.de/v2/flights/arrivals"
    api_key = AIRPORT_API_KEY
    print(f"Collecting remote data from {url} ...")
    request = requests.get(url, headers={"Ocp-Apim-Subscription-Key": api_key})
    events = parse_airport_arrivals(request.json())
    print(events)
    events_points = [event.to_point() for event in events]
    print(
        f"Writing to timeseries db {[event.to_line_protocol() for event in events_points]} ..."
    )
    write_points_to_influx(bucket, events_points)
