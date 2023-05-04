from functools import cache
import os
from datetime import datetime, timedelta
from typing import Callable, Collection, Dict, List, Sequence, Union

import requests

from influxdb import write_points_to_influx
from models import AirQualityMeasurmentStation, DWDWeatherStation, Location, Pointable
from parse import (
    extract_air_quality,
    extract_bike_traffic_status,
    extract_ev_charging_events,
    extract_parking_usage,
    extract_stadtrad_stations,
    extract_traffic_counts,
    extract_traffic_status,
    extract_weather_sensors,
    parse_air_quality_measurments,
    parse_airport_arrivals,
    parse_dwd_weather_event,
)
from utils import now_germany

AIRPORT_API_KEY = os.getenv("AIRPORT_API_KEY", "NO_KEY_PROVIDED")


@cache
def get_remote_events(url: str, json=True, headers: tuple = ()):
    print(f"Collecting remote data from {url} ...")
    headers_dict = dict([headers]) if headers else {}
    request = requests.get(url, headers=headers_dict)
    return request.json() if json else request.text


def extract_transform_load_hamburg_geodienste(
    bucket: str, url: str, extract_function: Callable
):
    try:
        result_xml = get_remote_events(url, json=False)
        print(f"Parsing events ...")
        events = extract_function(result_xml)
        load_events(bucket, events)
    except Exception as e:
        print(
            "Something went wrong while doing: fetch_and_transform_geoportal_events", e
        )


def extract_hamburg_iot_events(url: str, acc_events=[]) -> List[Union[str, Dict]]:
    print(f"Collecting remote data from {url} ...")
    result_json: Dict = get_remote_events(url, json=True)  # type: ignore
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


def load_events(bucket: str, events: Collection[Pointable]):
    print("loading events")
    events_points = [event.to_point() for event in events]
    print(f"Writing {len(events_points)} events to the timeseries db in bucket {bucket} ...")
    write_points_to_influx(bucket, events_points)


def extract_transform_load_hamburg_iot(
    bucket: str, url: str, transform_function: Callable[[str], Sequence[Pointable]]
):
    try:
        results = extract_hamburg_iot_events(url)
        print(f"Transforming ~{len(results)}x100 events")
        transformed_events = transform_events(results, transform_function)
        load_events(bucket, transformed_events)
    except Exception as e:
        print("Something went wrong while doing: extract_transform_load_hamburg_iot", e)


def collect_stadtrad(bucket: str):

    if datetime.datetime.now().hour >= 1 and datetime.datetime.now().hour < 4:
        print("Skipping stadtrad collection, as we were asked to limit calls in this time")
        return

    extract_transform_load_hamburg_geodienste(
        bucket,
        "https://geodienste.hamburg.de/HH_WFS_Stadtrad?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typename=de.hh.up:stadtrad_stationen",
        extract_stadtrad_stations,
    )


def collect_parking_usage(bucket: str):
    extract_transform_load_hamburg_geodienste(
        bucket,
        "https://geodienste.hamburg.de/HH_WFS_Verkehr_opendata?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typename=de.hh.up:verkehr_parkhaeuser",
        extract_parking_usage,
    )


def collect_air_quality(bucket: str):
    extract_transform_load_hamburg_geodienste(
        bucket,
        "https://geodienste.hamburg.de/HH_WFS_Luftmessnetz?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&typename=app:luftmessnetz_messwerte",
        extract_air_quality,
    )


def collect_swis(bucket: str):
    extract_transform_load_hamburg_geodienste(
        bucket,
        "https://geodienste.hamburg.de/DE_HH_INSPIRE_WFS_SWIS_Sensoren?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&typename=app:swis_sensoren",
        extract_weather_sensors,
    )


def collect_e_charging_stations(bucket: str):
    extract_transform_load_hamburg_iot(
        bucket,
        "https://iot.hamburg.de/v1.0/Things?$filter=Datastreams/properties/serviceName eq 'HH_STA_E-Ladestationen'&$count=true&$expand=Locations,Datastreams($expand=Observations($top=1;$orderby=phenomenonTime desc),Sensor,ObservedProperty)",
        extract_ev_charging_events,
    )


def collect_traffic_counts(bucket: str):
    extract_transform_load_hamburg_iot(
        bucket,
        "https://iot.hamburg.de/v1.1/Things?$filter=Datastreams/properties/serviceName eq 'HH_STA_AutomatisierteVerkehrsmengenerfassung'  and Datastreams/properties/layerName eq 'Anzahl_Kfz_Zaehlstelle_15-Min' &$count=true&$expand=Datastreams($filter=properties/layerName eq 'Anzahl_Kfz_Zaehlstelle_15-Min';$expand=Observations($top=1;$orderby=phenomenonTime desc))",
        extract_traffic_counts,
    )

def collect_traffic_status(bucket: str):
    extract_transform_load_hamburg_geodienste(
        bucket,
        "https://geodienste.hamburg.de/HH_WFS_Verkehrslage?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&typename=de.hh.up:verkehrslage",
        extract_traffic_status,
    )


def collect_bike_traffic_status(bucket: str):
    extract_transform_load_hamburg_iot(
        bucket,
        "https://iot.hamburg.de/v1.1/Things?$filter=Datastreams/properties/serviceName eq 'HH_STA_HamburgerRadzaehlnetz' and Datastreams/properties/layerName eq 'Anzahl_Fahrraeder_Zaehlstelle_15-Min'&$count=true&$expand=Datastreams($filter=properties/layerName eq 'Anzahl_Fahrraeder_Zaehlstelle_15-Min';$expand=Observations($top=1;$orderby=phenomenonTime desc))",
        extract_bike_traffic_status,
    )


def collect_airport_arrivals(bucket: str):
    url = "https://rest.api.hamburg-airport.de/v2/flights/arrivals"
    api_key = AIRPORT_API_KEY
    request_json = get_remote_events(
        url, json=True, headers=("Ocp-Apim-Subscription-Key", api_key)
    )
    events = parse_airport_arrivals(request_json)
    load_events(bucket, events)


def collect_detailed_air_quality_hamburg(
    bucket: str, station: AirQualityMeasurmentStation
):
    now = now_germany()
    date = datetime.strftime(now, "%d.%m.%Y")
    one_hour_ago = now - timedelta(hours=1)
    url = f"https://hamburg.luftmessnetz.de/station/{station.station_id}.csv?group=pollution&period=1h&timespan=custom&start[date]={date}&start[hour]={one_hour_ago.hour}&end[date]={date}&end[hour]={now.hour}"
    try:
        print(f"Collecting remote data from {url} ...")
        result_csv = get_remote_events(url, json=False)
        print(f"Parsing events from csv...")
        parsed_result = parse_air_quality_measurments(result_csv, station)
        print(f"Transforming {len(parsed_result)} events")
        load_events(bucket, parsed_result)
    except Exception as e:
        print(
            "Something went wrong while doing: collect_detailed_air_quality_hamburg", e
        )


def collect_detailed_air_quality_hamburg_list(bucket: str) -> None:
    stations = [
        AirQualityMeasurmentStation(
            station_id="64ks", location=Location(lat=562563.000, lon=5935470.000)
        ),
        AirQualityMeasurmentStation(
            station_id="17sm", location=Location(lat=562563.000, lon=5935470.000)
        ),
        AirQualityMeasurmentStation(
            station_id="70mb", location=Location(lat=562473.000, lon=5934507.000)
        ),
        AirQualityMeasurmentStation(
            station_id="68hb", location=Location(lat=569743.000, lon=5938684.000)
        ),
    ]
    for station in stations:
        collect_detailed_air_quality_hamburg(bucket, station)


def collect_weather_dwd(station: DWDWeatherStation, bucket: str):
    try:
        events_json = get_remote_events(
            f"https://s3.eu-central-1.amazonaws.com/app-prod-static.warnwetter.de/v16/current_measurement_{station.id}.json"
        )
        event = parse_dwd_weather_event(events_json, station)  # type: ignore
        print(event.to_point().to_line_protocol())
        load_events(bucket, [event])
    except Exception as e:
        print("Something went wrong while doing: collect_weather_dwd", e)
