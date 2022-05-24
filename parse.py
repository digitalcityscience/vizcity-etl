from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import jmespath
import xmltodict

from models import (
    AirQuality,
    BikeTrafficStatus,
    StadtradStation,
    TrafficStatus,
    WeatherSensor,
)

GERMANY_TIMEZONE = timezone(+timedelta(hours=2))


def parse_timestamp_like(timestamp_like: float) -> datetime:
    return datetime.strptime(
        str(round(float(timestamp_like), 6)), "%Y%m%d%H%M%S.%f"
    ).astimezone(GERMANY_TIMEZONE)

def parse_date_time(date: str, time: str) -> datetime:
    return datetime.strptime(
        f"{date}{time}",
        "%Y-%m-%d%H:%M:%S",
    ).astimezone(GERMANY_TIMEZONE)

@dataclass
class EvChargingStationEvent:
    status: str
    address: str
    lat: float
    lon: float
    timestamp: int


def extract_ev_charging_events(json_data: str) -> List[EvChargingStationEvent]:
    result = jmespath.search(
        "value[*].{status:Datastreams[0].Observations[0].result, lon: Locations[0].location.geometry.coordinates[0], lat: Locations[0].location.geometry.coordinates[1], address:Locations[0].description, timestamp:Datastreams[0].Observations[0].phenomenonTime}",
        json_data,
    )
    return result  # type: ignore


@dataclass
class Parking:
    name: str
    utilization: str
    lat: float
    lon: float


def extract_parking_usage(xml_data: str) -> List[Parking]:
    collapsed_namespaces = {
        "http://www.opengis.net/wfs/2.0:member": "member",
        "http://www.opengis.net/wfs/2.0:FeatureCollection": "collection",
    }
    xml = xmltodict.parse(
        xml_data, process_namespaces=False, namespaces=collapsed_namespaces
    )
    entries = [
        entry["de.hh.up:verkehr_parkhaeuser"]
        for entry in xml["wfs:FeatureCollection"]["wfs:member"]
    ]

    def remap_entry(xml_entry):
        location = (
            xml_entry.get("de.hh.up:position", {})
            .get("gml:Point", {})
            .get("gml:pos", "0 0")
            .split()
        )
        return Parking(
            name=xml_entry["de.hh.up:name"],
            utilization=xml_entry.get("de.hh.up:situation", "no_data"),
            lat=location[0],
            lon=location[1],
        )

    return list(map(remap_entry, entries))


@dataclass
class ParkingLazyTown:
    name: str
    utilization: str
    lat: float
    lon: float
    free: int
    capacity: int
    price: str
    timestamp: Any


from datetime import datetime


def extract_parking_usage_lazytown(xml_data: str) -> List[ParkingLazyTown]:
    collapsed_namespaces = {
        "http://www.opengis.net/wfs/2.0:member": "member",
        "http://www.opengis.net/wfs/2.0:FeatureCollection": "collection",
    }
    xml = xmltodict.parse(
        xml_data, process_namespaces=False, namespaces=collapsed_namespaces
    )
    entries = [
        entry["de.hh.up:verkehr_parkhaeuser"]
        for entry in xml["wfs:FeatureCollection"]["wfs:member"]
    ]

    def remap_entry(xml_entry):
        location = (
            xml_entry.get("de.hh.up:position", {})
            .get("gml:Point", {})
            .get("gml:pos", "0 0")
            .split()
        )
        return ParkingLazyTown(
            name=xml_entry["de.hh.up:name"],
            utilization=xml_entry.get("de.hh.up:situation", "no_data"),
            lat=float(location[0]),
            lon=float(location[1]),
            free=xml_entry.get("de.hh.up:frei", 0),
            capacity=xml_entry.get("de.hh.up:gesamt", 0),
            price=xml_entry.get("de.hh.up:preise", ""),
            timestamp=datetime.now(),  # xml_entry.get("de.hh.up:received"),
        )

    return list(map(remap_entry, entries))


def extract_stadtrad_stations(xml_data: str) -> List[StadtradStation]:
    xml = xmltodict.parse(xml_data, process_namespaces=False)
    entries = [
        entry["de.hh.up:stadtrad_stationen"]
        for entry in xml["wfs:FeatureCollection"]["wfs:member"]
    ]

    def remap_entry(xml_entry) -> StadtradStation:
        location = (
            xml_entry.get("de.hh.up:geom", {})
            .get("gml:Point", {})
            .get("gml:pos", "0 0")
            .split()
        )
        return StadtradStation(
            name=xml_entry["de.hh.up:name"],
            lat=float(location[0]),
            lon=float(location[1]),
            count=int(xml_entry.get("de.hh.up:anzahl_raeder", 0)),
            count_bike=int(xml_entry.get("de.hh.up:anzahl_bike", 0)),
            count_pedelec=int(xml_entry.get("de.hh.up:anzahl_pedelec", 0)),
            count_cargobike_electric=int(
                xml_entry.get("de.hh.up:anzahl_cargobike_electric", 0)
            ),
            timestamp=parse_timestamp_like(xml_entry.get("de.hh.up:stand")),
        )

    return list(map(remap_entry, entries))


def extract_weather_sensors(xml_data: str) -> List[WeatherSensor]:
    xml = xmltodict.parse(xml_data, process_namespaces=False)
    entries = [
        entry["app:swis_sensoren"]
        for entry in xml["wfs:FeatureCollection"]["gml:featureMember"]
    ]
    timestamp = xml.get("wfs:FeatureCollection", {}).get("@timeStamp", datetime.now())

    def remap_entry(xml_entry) -> WeatherSensor:
        location = (
            xml_entry.get("app:geom", {})
            .get("gml:Point", {})
            .get("gml:pos", "0 0")
            .split()
        )
        return WeatherSensor(
            station=xml_entry["app:station"],
            street=xml_entry["app:strasse"],
            vonnullpunkt=int(xml_entry.get("app:vonnullpunkt", 0)),
            nachnullpunkt=int(xml_entry.get("app:nachnullpunkt", 0)),
            lat=float(location[0]),
            lon=float(location[1]),
            timestamp=timestamp,
        )

    return list(map(remap_entry, entries))


def remap_location(xml_entry: Dict):
    location = (
        xml_entry.get("app:geom", {}).get("gml:Point", {}).get("gml:pos", "0 0").split()
    )
    return float(location[0]), float(location[1])


def extract_air_quality(xml_data: str) -> List[AirQuality]:
    xml = xmltodict.parse(xml_data, process_namespaces=False)
    entries = [
        entry["app:luftmessnetz_messwerte"]
        for entry in xml["wfs:FeatureCollection"]["gml:featureMember"]
    ]

    def remap_entry(xml_entry) -> AirQuality:
        return AirQuality(
            station_id=xml_entry["app:stationskuerzel"],
            street=xml_entry["app:adresse"],
            name=xml_entry["app:name"],
            station_type=xml_entry["app:stationstyp"],
            lqi=float(xml_entry.get("app:LQI", 0)),
            no2=float(xml_entry.get("app:NO2", 0)),
            so2=float(xml_entry.get("app:SO2", 0)),
            pm10=float(xml_entry.get("app:PM10", 0)),
            lat=remap_location(xml_entry)[0],
            lon=remap_location(xml_entry)[1],
            timestamp=parse_date_time(
                date=xml_entry.get("app:datum"), time=xml_entry.get("app:messzeit")
            ),
        )

    return list(map(remap_entry, entries))


def extract_traffic_status(json_data: str) -> List[TrafficStatus]:
    results = jmespath.search(
        "value[*].{counted_traffic:Datastreams[0].Observations[0].result, lon: Datastreams[0].observedArea.coordinates[0], lat: Datastreams[0].observedArea.coordinates[1], timestamp:Datastreams[0].Observations[0].resultTime}",
        json_data,
    )
    return list(map(lambda result: TrafficStatus(**result), results))  # type: ignore


def extract_bike_traffic_status(json_data: str) -> List[TrafficStatus]:
    results = jmespath.search(
        "value[*].{counted_traffic:Datastreams[0].Observations[0].result, lon: Datastreams[0].observedArea.coordinates[0], lat: Datastreams[0].observedArea.coordinates[1], timestamp:Datastreams[0].Observations[0].resultTime}",
        json_data,
    )
    return list(map(lambda result: BikeTrafficStatus(**result), results))  # type: ignore
