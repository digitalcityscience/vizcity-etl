import json
import os
from parse import extract_ev_charging_events, extract_parking_usage


def test_extract_ev_charging_events(snapshot):
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "ev-charging-stations.json"
    )
    with open(fixture_file) as json_file:
        data = json.load(json_file)
        assert extract_ev_charging_events(data) == snapshot


def test_extract_parking_usage(snapshot):
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "verkehr_parkhaeuser.xml"
    )
    with open(fixture_file) as xml_file:
        assert extract_parking_usage(xml_file.read()) == snapshot
