from datetime import date, datetime, timezone
from xml.etree.ElementTree import fromstring

from influxdb_client import Point

from models import (
    AirQuality,
    AirportArrival,
    StadtradStation,
    TrafficStatus,
    LocationEPSG,
    WeatherConditions,
)


def test_stadtrad_station_to_point():
    given = StadtradStation(
        name="Enckeplatz / Hütten",
        count=0,
        count_pedelec=0,
        count_bike=0,
        count_cargobike_electric=0,
        lat=564722.695,
        lon=5934167.221,
        location_EPSG=LocationEPSG.from_single_line("564722.695 5934167.221"),
        timestamp=datetime.now(),
    )
    expected = (
        Point("stadtrad_station")
        .tag("name", given.name)
        .field("count_pedelec", given.count_pedelec)
        .field("count_bike", given.count_bike)
        .field("count_cargobike_electric", given.count_cargobike_electric)
        .field("count", given.count)
        .field("location_EPSG", given.location_EPSG.system)
        .field("location_EPSG_x", given.location_EPSG.x)
        .field("location_EPSG_y", given.location_EPSG.y)
        .tag("lat", given.lat)
        .tag("lon", given.lon)
        .time(given.timestamp)
    )
    assert expected.to_line_protocol() == given.to_point().to_line_protocol()


def test_traffic_status_to_point():
    given = TrafficStatus(
        lat=53.580056,
        lon=9.999282,
        counted_traffic=198,
        timestamp=datetime.now(),
        station_id="123 Test",
    )

    expected = (
        Point("kfz_verkehr")
        .field("counted_traffic", given.counted_traffic)
        .tag("lat", given.lat)
        .tag("lon", given.lon)
        .tag("station_id", given.station_id)
        .time(given.timestamp)
    )
    assert expected.to_line_protocol() == given.to_point().to_line_protocol()


def test_air_quality_to_point():
    given = AirQuality(
        lat=562609.0,
        lon=5933343.0,
        location_EPSG=LocationEPSG.from_single_line("562609.0 5933343.0"),
        name="Altona-Elbhang",
        station_type="Hintergrundmessstation",
        station_id="80KT",
        street="Olbertsweg, am Park",
        lqi=1.0,
        no2=1.0,
        so2=1.0,
        pm10=1.0,
        timestamp=datetime(2022, 5, 23, 16, 0, tzinfo=timezone.utc),
    )
    expected = "luftmessnetz_messwerte,lat=562609.0,lon=5933343.0,name=Altona-Elbhang,station_id=80KT,station_type=Hintergrundmessstation,street=Olbertsweg\,\ am\ Park location_EPSG=25832i,location_EPSG_x=562609,location_EPSG_y=5933343,lqi=1,no2=1,pm10=1,so2=1 1653321600000000000"

    assert datetime.fromtimestamp(1653321600, timezone.utc) == datetime(
        2022, 5, 23, 16, 0, tzinfo=timezone.utc
    )
    assert expected == given.to_point().to_line_protocol()


def test_airport_arrival_to_point():
    given = AirportArrival.from_dict(
        {
            "plannedArrivalTime": "2022-05-23T13:00:00.000+02:00[Europe/Berlin]",
            "viaAirport3LCode": None,
            "viaAirportName": None,
            "originAirport3LCode": "STR",
            "originAirportName": "Stuttgart",
            "originAirportLongName": "Stuttgart",
            "originAirportNameInt": "Stuttgart",
            "originAirportLongNameInt": "Stuttgart",
            "flightnumber": "EW 2040",
            "arrivalTerminal": "1",
            "expectedArrivalTime": "2022-05-23T14:05:40.000+02:00[Europe/Berlin]",
            "flightStatusArrival": "ONB",
            "codeShareInfoFlightnumber": [],
            "airline2LCode": "EW",
            "airlineName": "Eurowings",
        }
    )
    expected = 'airport_arrivals,airline2LCode=EW,airlineName=Eurowings,arrivalTerminal=1,flightnumber=EW\ 2040,originAirport3LCode=STR,originAirportName=Stuttgart originAirportName="Stuttgart" 1653307540000000000'
    assert given.to_point().to_line_protocol() == expected


def test_airport_arrival_to_point_without_expected_arrival_time():
    given = AirportArrival.from_dict(
        {
            "plannedArrivalTime": "2022-05-23T13:00:00.000+02:00[Europe/Berlin]",
            "viaAirport3LCode": None,
            "viaAirportName": None,
            "originAirport3LCode": "STR",
            "originAirportName": "Stuttgart",
            "originAirportLongName": "Stuttgart",
            "originAirportNameInt": "Stuttgart",
            "originAirportLongNameInt": "Stuttgart",
            "flightnumber": "EW 2040",
            "arrivalTerminal": "1",
            "expectedArrivalTime": None,
            "flightStatusArrival": "ONB",
            "codeShareInfoFlightnumber": [],
            "airline2LCode": "EW",
            "airlineName": "Eurowings",
        }
    )
    expected = 'airport_arrivals,airline2LCode=EW,airlineName=Eurowings,arrivalTerminal=1,flightnumber=EW\ 2040,originAirport3LCode=STR,originAirportName=Stuttgart originAirportName="Stuttgart" 1653303600000000000'
    assert given.to_point().to_line_protocol() == expected


def test_weather_condition_to_point():
    given = WeatherConditions(
        comment="Sunny", precipitation=11, temperature=23, wind_speed=13, timestamp=datetime.now(), region="Hamburg"
    )

    expected = (
        Point("weather")
        .field("temperature", given.temperature)
        .tag("precipitation", given.precipitation)
        .tag("wind_speed", given.wind_speed)
        .tag("region", given.region)
        .tag("comment", given.comment)
        .time(given.timestamp)
    )
    assert expected.to_line_protocol() == given.to_point().to_line_protocol()
