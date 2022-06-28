import json
import os

from etl import (
    transform_events,
)
from parse import extract_ev_charging_events


def test_transform_events():
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "ev-charging-stations.json"
    )
    with open(fixture_file) as json_file:
        data = json.load(json_file)
        result = transform_events([data, data], extract_ev_charging_events)
        assert len(result) == 200
