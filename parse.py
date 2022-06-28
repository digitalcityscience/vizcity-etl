import csv
from datetime import datetime
from typing import Dict, List, Union

import jmespath
import xmltodict

from models import (
    AirportArrival,
    AirQuality,
    AirQualityMeasurment,
    AirQualityMeasurmentStation,
    BikeTrafficStatus,
    DWDWeatherStation,
    EvChargingStationEvent,
    LocationEPSG,
    Parking,
    StadtradStation,
    TrafficStatus,
    WeatherConditions,
    WeatherSensor,
)
from utils import (
    from_epsg25832_to_gps,
    parse_date_comma_time,
    parse_date_time,
    parse_date_time_without_seconds,
    parse_day_time_relative,
    parse_timestamp_like,
)


def extract_ev_charging_events(
    json_data: Union[str, Dict]
) -> List[EvChargingStationEvent]:
    results = jmespath.search(
        "value[*].{status:Datastreams[0].Observations[0].result, lon: Locations[0].location.geometry.coordinates[0], lat: Locations[0].location.geometry.coordinates[1], address:Locations[0].description, timestamp:Datastreams[0].Observations[0].phenomenonTime, station_id:properties.assetID}",
        json_data,
    )
    return list(map(lambda result: EvChargingStationEvent(**result), results))  # type: ignore


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

    fallback_timestamp = xml.get("wfs:FeatureCollection", {}).get(
        "@timeStamp", datetime.now()
    )

    def remap_entry(xml_entry):
        location_epsg25832 = remap_location(xml_entry, "de.hh.up:position")
        location = from_epsg25832_to_gps(location_epsg25832.x, location_epsg25832.y)
        timestamp = (
            fallback_timestamp
            if not xml_entry.get("de.hh.up:received")
            else parse_date_comma_time(xml_entry.get("de.hh.up:received"))
        )
        return Parking(
            name=xml_entry["de.hh.up:name"],
            utilization=xml_entry.get("de.hh.up:situation", "no_data"),
            lat=location.get("lat", 0),
            lon=location.get("lon", 0),
            location_EPSG=location_epsg25832,
            free=int(xml_entry.get("de.hh.up:frei", 0)),
            capacity=int(xml_entry.get("de.hh.up:gesamt", 0)),
            price=xml_entry.get("de.hh.up:preise", ""),
            timestamp=timestamp,
        )

    return list(map(remap_entry, entries))


def extract_stadtrad_stations(xml_data: str) -> List[StadtradStation]:
    xml = xmltodict.parse(xml_data, process_namespaces=False)
    entries = [
        entry["de.hh.up:stadtrad_stationen"]
        for entry in xml["wfs:FeatureCollection"]["wfs:member"]
    ]

    def remap_entry(xml_entry) -> StadtradStation:
        location_epsg25832 = remap_location(xml_entry, "de.hh.up:geom")
        location = from_epsg25832_to_gps(location_epsg25832.x, location_epsg25832.y)

        return StadtradStation(
            name=xml_entry["de.hh.up:name"],
            lat=location.get("lat", 0),
            lon=location.get("lon", 0),
            location_EPSG=location_epsg25832,
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
        location_epsg25832 = remap_location(xml_entry, "app:geom")
        location = from_epsg25832_to_gps(location_epsg25832.x, location_epsg25832.y)
        return WeatherSensor(
            station=xml_entry["app:station"],
            street=xml_entry["app:strasse"],
            vonnullpunkt=int(xml_entry.get("app:vonnullpunkt", 0)),
            nachnullpunkt=int(xml_entry.get("app:nachnullpunkt", 0)),
            lat=location.get("lat", 0),
            lon=location.get("lon", 0),
            location_EPSG=location_epsg25832,
            timestamp=timestamp,
        )

    return list(map(remap_entry, entries))


def remap_location(xml_entry: Dict, tag_name: str) -> LocationEPSG:
    return LocationEPSG.from_single_line(
        xml_entry.get(tag_name, {}).get("gml:Point", {}).get("gml:pos", "0 0")
    )


def extract_air_quality(xml_data: str) -> List[AirQuality]:
    xml = xmltodict.parse(xml_data, process_namespaces=False)
    entries = [
        entry["app:luftmessnetz_messwerte"]
        for entry in xml["wfs:FeatureCollection"]["gml:featureMember"]
    ]

    def remap_entry(xml_entry) -> AirQuality:
        location_epsg25832 = remap_location(xml_entry, "app:geom")
        location = from_epsg25832_to_gps(location_epsg25832.x, location_epsg25832.y)

        return AirQuality(
            station_id=xml_entry["app:stationskuerzel"],
            street=xml_entry["app:adresse"],
            name=xml_entry["app:name"],
            station_type=xml_entry["app:stationstyp"],
            lqi=float(xml_entry.get("app:LQI", 0)),
            no2=float(xml_entry.get("app:NO2", 0)),
            so2=float(xml_entry.get("app:SO2", 0)),
            pm10=float(xml_entry.get("app:PM10", 0)),
            lat=location.get("lat", 0),
            lon=location.get("lon", 0),
            location_EPSG=location_epsg25832,
            timestamp=parse_date_time(
                date=xml_entry.get("app:datum"), time=xml_entry.get("app:messzeit")
            ),
        )

    return list(map(remap_entry, entries))


def extract_traffic_status(json_data: str) -> List[TrafficStatus]:
    results = jmespath.search(
        "value[*].{counted_traffic:Datastreams[0].Observations[0].result, lon: Datastreams[0].observedArea.coordinates[0], lat: Datastreams[0].observedArea.coordinates[1], timestamp:Datastreams[0].Observations[0].resultTime, station_id:properties.assetID}",
        json_data,
    )
    return list(map(lambda result: TrafficStatus(**result), results))  # type: ignore


def extract_e_charging_stations(json_data: str) -> List[TrafficStatus]:
    results = jmespath.search(
        "value[*].{counted_traffic:Datastreams[0].Observations[0].result, lon: Datastreams[0].observedArea.coordinates[0], lat: Datastreams[0].observedArea.coordinates[1], timestamp:Datastreams[0].Observations[0].resultTime}",
        json_data,
    )
    return list(map(lambda result: TrafficStatus(**result), results))  # type: ignore


def extract_bike_traffic_status(json_data: str) -> List[TrafficStatus]:
    results = jmespath.search(
        "value[*].{counted_traffic:Datastreams[0].Observations[0].result, lon: Datastreams[0].observedArea.coordinates[0], lat: Datastreams[0].observedArea.coordinates[1], timestamp:Datastreams[0].Observations[0].resultTime, station_id:name}",
        json_data,
    )
    return list(map(lambda result: BikeTrafficStatus(**result), results))  # type: ignore


def parse_airport_arrivals(json_data: str) -> List[AirportArrival]:
    return list(map(lambda result: AirportArrival.from_dict(result), json_data))


def parse_air_quality_measurments(
    csv_plain: str, station: AirQualityMeasurmentStation
) -> List[AirQualityMeasurment]:
    result = []
    csv_data = csv.reader(csv_plain.splitlines(), delimiter=";")
    station_street = ""
    for idx, csv_entry in enumerate(csv_data):
        if idx == 0:
            station_street = csv_entry[1]
        if idx > 4:
            result.append(
                AirQualityMeasurment(
                    timestamp=parse_date_time_without_seconds(csv_entry[0]),
                    street=station_street,
                    no2=int(csv_entry[1] or 0),
                    no2_4m=int(csv_entry[2] or 0),
                    no=int(csv_entry[3] or 0),
                    no_4m=int(csv_entry[4] or 0),
                    station=station,
                )
            )
    return result


def parse_dwd_weather_event(
    current_measurements: Dict, station: DWDWeatherStation
) -> WeatherConditions:
    return WeatherConditions(
        precipitation=float(current_measurements.get("precipitation", "0")),
        temperature=current_measurements.get("temperature", 0),        pressure=current_measurements.get("pressure", 0),
        wind_speed=current_measurements.get("meanwind", 0),
        timestamp=current_measurements.get("time", datetime.now()),
        humidity=current_measurements.get("humidity", 0),
        station=station,
    )
