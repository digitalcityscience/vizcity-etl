import json
import os
from datetime import datetime

from models import (
    AirQualityMeasurment,
    AirQualityMeasurmentStation,
    DWDWeatherStation,
    Location,
    WeatherConditions,
)
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
from utils import GERMANY_TIMEZONE

def test_extract_extract_traffic_status(snapshot):
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "verkehrslage.xml"
    )
    with open(fixture_file) as xml_file:
        result = extract_traffic_status(xml_file.read())
        assert "2022-05-23T17:20:00" == result[0].timestamp
        assert result == snapshot

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


def test_extract_traffic_counts(snapshot):
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "traffic_status.json"
    )
    with open(fixture_file) as json_file:
        data = json.load(json_file)
        assert extract_traffic_counts(data) == snapshot


def test_parse_airport_arrivals(snapshot):
    fixture_file = os.path.join(os.path.dirname(__file__), "fixtures", "arrivals.json")
    with open(fixture_file) as json_file:
        data = json.load(json_file)
        assert parse_airport_arrivals(data) == snapshot


def test_extract_bike_traffic_status(snapshot):
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "HH_STA_HamburgerRadzaehlnetz.json"
    )
    with open(fixture_file) as json_file:
        data = json.load(json_file)
        assert extract_bike_traffic_status(data) == snapshot


def test_parse_air_quality_measurments(snapshot):
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "air_quality_measurments.csv"
    )
    with open(fixture_file) as csv_file:
        station = AirQualityMeasurmentStation("testId", Location(0, 0))
        result = parse_air_quality_measurments(csv_file.read(), station)
        first_result = AirQualityMeasurment(
            timestamp=datetime(2022, 5, 31, 10, 0).astimezone(GERMANY_TIMEZONE),
            street="Kieler Straße",
            no2=56,
            no2_4m=58,
            no=42,
            no_4m=36,
            station=station,
        )
        assert len(result) == 168
        assert first_result == result[0]
        assert result == snapshot


def test_parse_dwd_weather_event():
    fixture_file = os.path.join(
        os.path.dirname(__file__), "fixtures", "dwd_weather.json"
    )
    with open(fixture_file) as json_file:
        data = json.load(json_file)
        station = DWDWeatherStation(
            location=Location(lat=53.38, lon=10.00),
            id="10147",
            name="HAMBURG-FU",
            elevation=16,
        )
        given = parse_dwd_weather_event(data, station)
        expected = WeatherConditions(
            precipitation=0,
            temperature=190,
            wind_speed=160,
            timestamp=datetime(year=2022, month=6, day=27, hour=17).astimezone(GERMANY_TIMEZONE),
            humidity=870,
            pressure=10149,
            station=station,
        )
        assert given == expected