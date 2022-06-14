from datetime import datetime, timezone
from utils import *


def test_parse_timestamp_like():
    assert parse_timestamp_like(20220524224141.8277977) == datetime(
        2022, 5, 24, 22, 41, 41, 830000
    ).astimezone(timezone.utc)


def test_parse_date_time():
    assert parse_date_time("2022-05-23", "16:00:00") == datetime(
        2022, 5, 23, 16, 0, 0
    ).astimezone(timezone.utc)

def test_parse_date_time_without_seconds():
    assert parse_date_time_without_seconds("31.05.2022 10:00") == datetime(
        2022, 5, 31, 10, 0, 0
    ).astimezone(timezone.utc)


def test_parse_date_comma_time():
    assert parse_date_comma_time("24.05.2022, 23:45") == datetime(
        2022, 5, 24, 23, 45, 0
    ).astimezone(timezone.utc)


def test_parse_date_with_timezone_text():
    assert parse_date_with_timezone_text(
        "2022-05-26T15:00:00.000+02:00[Europe/Berlin]"
    ) == datetime(2022, 5, 26, 15, 0, 0).astimezone(timezone.utc)

def test_parse_date_with_timezone_text_utc():
    assert parse_date_with_timezone_text(
        "2022-05-26T15:00:00.000+02:00[UTC]"
    ) == datetime(2022, 5, 26, 15, 0, 0).astimezone(timezone.utc)


def test_reproject_epsg25832_location():
    assert from_epsg25832_to_gps(x=562609.000, y=5933343.000) == { "lat":53.5452487, "lon":9.9448849 }