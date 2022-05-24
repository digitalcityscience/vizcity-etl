from typing import Callable

import requests

from influxdb import write_points_to_influx


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
