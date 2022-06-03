import json
import os

from etl import (
    extract_hamburg_iot_events,
    transform_events,
)
from parse import extract_ev_charging_events


def test_integration_extract_hamburg_iot_events():
    res = extract_hamburg_iot_events(
        "https://iot.hamburg.de/v1.0/Things?$filter=Datastreams/properties/serviceName eq 'HH_STA_E-Ladestationen'&$count=true&$expand=Locations,Datastreams($expand=Observations($top=1;$orderby=phenomenonTime desc))"
    )
    assert len(res) == 7


def test_transform_events():
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "ev-charging-stations.json"
    )
    with open(fixture_file) as json_file:
        data = json.load(json_file)
        result = transform_events([data, data], extract_ev_charging_events)
        assert len(result) == 200
