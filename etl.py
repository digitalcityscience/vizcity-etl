import os
from typing import Any, Callable, Dict, Iterable, List, Sequence, Union

import requests
from influxdb_client import Point

from influxdb import write_points_to_influx
from models import Pointable
from parse import (
    extract_air_quality,
    extract_bike_traffic_status,
    extract_ev_charging_events,
    extract_parking_usage,
    extract_stadtrad_stations,
    extract_traffic_status,
    extract_weather_sensors,
    parse_airport_arrivals,
)

AIRPORT_API_KEY = os.getenv("AIRPORT_API_KEY", "NO_KEY_PROVIDED")


def fetch_and_transform_geoportal_events(
    bucket: str, url: str, extract_function: Callable, json=False
):
    try:
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
    except Exception as e:
        print(
            "Something went wrong while doing: fetch_and_transform_geoportal_events", e
        )


def extract_hamburg_iot_events(url: str, acc_events=[]) -> List[Union[str, Dict]]:
    print(f"Collecting remote data from {url} ...")
    request = requests.get(url)
    result_json = request.json()
    next_page = result_json.get("@iot.nextLink")
    print(f"Found next-page? {next_page is not None}")
    result = [*acc_events, result_json]
    if next_page is not None:
        return extract_hamburg_iot_events(next_page, result)
    return result


def transform_events(
    events: List[Union[Dict, str]],
    transform_function: Callable[..., Sequence[Pointable]],
) -> List[Pointable]:
    result = list()
    for event in events:
        result.extend(transform_function(event))
    return result


def load_events_hamburg_iot(bucket: str, events: List[Pointable]):
    events_points = [event.to_point() for event in events]
    print(f"Writing {len(events_points)} events to the timeseries db ...")
    write_points_to_influx(bucket, events_points)


def extract_transform_load_hamburg_iot(
    bucket: str, url: str, transform_function: Callable[[str], List[Pointable]]
):
    try:
        results = extract_hamburg_iot_events(url)
        print(f"Transforming ~{len(results)}x100 events")
        transformed_events = transform_events(results, transform_function)
        load_events_hamburg_iot(bucket, transformed_events)
    except Exception as e:
        print("Something went wrong while doing: extract_transform_load_hamburg_iot", e)


def collect_stadtrad(bucket: str):
    fetch_and_transform_geoportal_events(
        bucket,
        "https://geodienste.hamburg.de/HH_WFS_Stadtrad?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typename=de.hh.up:stadtrad_stationen",
        extract_stadtrad_stations,
    )


def collect_parking_usage(bucket: str):
    fetch_and_transform_geoportal_events(
        bucket,
        "https://geodienste.hamburg.de/HH_WFS_Verkehr_opendata?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typename=de.hh.up:verkehr_parkhaeuser",
        extract_parking_usage,
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


def collect_traffic_status(bucket: str):
    extract_transform_load_hamburg_iot(
        bucket,
        "https://iot.hamburg.de/v1.1/Things?$filter=Datastreams/properties/serviceName eq 'HH_STA_AutomatisierteVerkehrsmengenerfassung'  and Datastreams/properties/layerName eq 'Anzahl_Kfz_Zaehlstelle_15-Min' &$count=true&$expand=Datastreams($filter=properties/layerName eq 'Anzahl_Kfz_Zaehlstelle_15-Min';$expand=Observations($top=1;$orderby=phenomenonTime desc))",
        extract_traffic_status,
    )


def collect_bike_traffic_status(bucket: str):
    fetch_and_transform_geoportal_events(
        bucket,
        "https://iot.hamburg.de/v1.1/Things?$filter=Datastreams/properties/serviceName eq 'HH_STA_HamburgerRadzaehlnetz' and Datastreams/properties/layerName eq 'Anzahl_Fahrraeder_Zaehlstelle_15-Min'&$count=true&$expand=Datastreams($filter=properties/layerName eq 'Anzahl_Fahrraeder_Zaehlstelle_15-Min';$expand=Observations($top=1;$orderby=phenomenonTime desc))",
        extract_bike_traffic_status,
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
