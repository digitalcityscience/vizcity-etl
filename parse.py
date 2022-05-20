import os
from dataclasses import dataclass
from typing import Any, List

import jmespath
import xmltodict


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
