import json
import os
from datetime import datetime

from parse import (
    extract_air_quality,
    extract_ev_charging_events,
    extract_parking_usage,
    extract_stadtrad_stations,
    extract_traffic_status,
    extract_weather_sensors,
)


def test_extract_ev_charging_events(snapshot):
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "ev-charging-stations.json"
    )
    with open(fixture_file) as json_file:
        data = json.load(json_file)
        result = extract_ev_charging_events(data)
        assert "2022-05-17T17:26:35.610Z" == result[0].timestamp
        assert result == snapshot


def test_extract_parking_usage(snapshot):
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "verkehr_parkhaeuser.xml"
    )
    with open(fixture_file) as xml_file:
        assert extract_parking_usage(xml_file.read()) == snapshot


def test_extract_stadtrad_stations(snapshot):
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "stadtrad_stationen.xml"
    )
    with open(fixture_file) as xml_file:
        assert extract_stadtrad_stations(xml_file.read()) == snapshot


def test_extract_weather_sensors(snapshot):
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "swis_sensoren.xml"
    )
    with open(fixture_file) as xml_file:
        result = extract_weather_sensors(xml_file.read())
        assert "2022-05-20T19:47:35Z" == result[0].timestamp
        assert result == snapshot


def test_extract_air_quality(snapshot):
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "luftmessnetz_messwerte.xml"
    )
    with open(fixture_file) as xml_file:
        result = extract_air_quality(xml_file.read())
        assert (
            datetime(2022, 5, 23, 16, 0).timestamp() == result[0].timestamp.timestamp()
        )
        assert result == snapshot


def test_extract_traffic_status(snapshot):
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "traffic_status.json"
    )
    with open(fixture_file) as json_file:
        data = json.load(json_file)
        assert extract_traffic_status(data) == snapshot
