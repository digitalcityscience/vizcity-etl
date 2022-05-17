import json
import os

from parse import extract_ev_charging_events


def test_extract_ev_charging_events():
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "ev-charging-stations.json"
    )
    with open(fixture_file) as json_file:
        data = json.load(json_file)
        assert extract_ev_charging_events(data) != None
