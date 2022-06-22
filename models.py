from ctypes import Union
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, Optional

from dataclasses_json import dataclass_json
from influxdb_client import Point

from utils import parse_date_with_timezone_text


@dataclass
class LocationEPSG:
    x: float
    y: float
    system: int = 25832

    @classmethod
    def from_single_line(cls, line: str = "0 0"):
        x, y = line.split()
        return LocationEPSG(float(x), float(y))


def add_location_EPSG_to_point(
    location_EPSG: Optional[LocationEPSG], point: Point
) -> None:
    if location_EPSG:
        point.field("location_EPSG", location_EPSG.system)
        point.field("location_EPSG_x", location_EPSG.x)
        point.field("location_EPSG_y", location_EPSG.y)


class Pointable(Protocol):
    def to_point(self) -> Point:
        raise NotImplementedError


@dataclass
class StadtradStation:
    name: str
    count: int
    count_pedelec: int
    count_bike: int
    count_cargobike_electric: int
    lat: float
    lon: float
    timestamp: datetime
    location_EPSG: Optional[LocationEPSG]

    def to_point(self) -> Point:
        point = (
            Point("stadtrad_station")
            .tag("name", self.name)
            .field("count", self.count)
            .field("count_pedelec", self.count_pedelec)
            .field("count_bike", self.count_bike)
            .field("count_cargobike_electric", self.count_cargobike_electric)
            .tag("lat", self.lat)
            .tag("lon", self.lon)
            .time(self.timestamp)
        )

        add_location_EPSG_to_point(self.location_EPSG, point)

        return point


@dataclass
class WeatherSensor:
    timestamp: str
    station: str
    street: str
    vonnullpunkt: int
    nachnullpunkt: int
    lat: float
    lon: float
    location_EPSG: Optional[LocationEPSG]

    def to_point(self) -> Point:
        point = (
            Point("swis_sensor")
            .tag("station", self.station)
            .tag("street", self.street)
            .tag("lat", self.lat)
            .tag("lon", self.lon)
            .field("nachnullpunkt", self.nachnullpunkt)
            .field("vonnullpunkt", self.vonnullpunkt)
            .time(self.timestamp)
        )

        add_location_EPSG_to_point(self.location_EPSG, point)
        return point


@dataclass
class Location:
    lat: float
    lon: float


@dataclass
class AirQuality(Location):
    name: str
    station_type: str
    station_id: str
    street: str
    lqi: float
    no2: float
    so2: float
    pm10: float
    timestamp: datetime
    location_EPSG: Optional[LocationEPSG]

    def to_point(self) -> Point:
        point = (
            Point("luftmessnetz_messwerte")
            .tag("name", self.name)
            .tag("station_id", self.station_id)
            .tag("street", self.street)
            .tag("station_type", self.station_type)
            .field("lqi", self.lqi)
            .field("no2", self.no2)
            .field("so2", self.so2)
            .field("pm10", self.pm10)
            .tag("lat", self.lat)
            .tag("lon", self.lon)
            .time(self.timestamp)
        )
        add_location_EPSG_to_point(self.location_EPSG, point)
        return point


@dataclass
class TrafficStatus(Location):
    timestamp: datetime
    counted_traffic: int
    measurement_name = "kfz_verkehr"
    station_id: str

    def to_point(self) -> Point:
        return (
            Point(self.measurement_name)
            .time(self.timestamp)
            .field("counted_traffic", self.counted_traffic)
            .tag("station_id", self.station_id)
            .tag("lat", self.lat)
            .tag("lon", self.lon)
        )


@dataclass
class BikeTrafficStatus(TrafficStatus):
    measurement_name = "fahrrad_verkehr"


@dataclass
class Parking(Location):
    name: str
    utilization: str
    free: int
    capacity: int
    price: str
    timestamp: Any
    location_EPSG: Optional[LocationEPSG]

    def to_point(self) -> Point:
        point = (
            Point("parking-spaces")
            .field("free", self.free)
            .tag("name", self.name)
            .tag("free", self.free)
            .tag("capacity", self.capacity)
            .tag("price", self.price)
            .tag("lat", self.lat)
            .tag("lon", self.lon)
            .time(self.timestamp)
        )
        add_location_EPSG_to_point(self.location_EPSG, point)
        return point


@dataclass
class EvChargingStationEvent(Location):
    status: str
    address: str
    timestamp: str
    station_id: str

    def to_point(self) -> Point:
        return (
            Point("ev-charging-stations")
            .field("status", self.status)
            .tag("address", self.address)
            .tag("lat", self.lat)
            .tag("station_id", self.station_id)
            .tag("lon", self.lon)
            .time(self.timestamp)
        )


@dataclass_json
@dataclass
class AirportArrival:
    expectedArrivalTime: str
    plannedArrivalTime: str
    originAirport3LCode: str
    originAirportName: str
    flightnumber: str
    arrivalTerminal: str
    airline2LCode: str
    airlineName: str

    def arrival_time(self) -> str:
        return (
            self.expectedArrivalTime
            if self.expectedArrivalTime is not None
            else self.plannedArrivalTime
        )

    def to_point(self) -> Point:
        return (
            Point("airport_arrivals")
            .field("originAirportName", self.originAirportName)
            .tag("originAirport3LCode", self.originAirport3LCode)
            .tag("originAirportName", self.originAirportName)
            .tag("flightnumber", self.flightnumber)
            .tag("arrivalTerminal", self.arrivalTerminal)
            .tag("airline2LCode", self.airline2LCode)
            .tag("airlineName", self.airlineName)
            .time(parse_date_with_timezone_text(self.arrival_time()))
        )


@dataclass
class AirQualityMeasurmentStation:
    station_id: str
    location: Location


@dataclass
class AirQualityMeasurment:
    street: str
    unit = "µg/m³"
    no: int
    no_4m: int
    no2: int
    no2_4m: int
    timestamp: datetime
    station: AirQualityMeasurmentStation

    def to_point(self) -> Point:
        return (
            Point("air_quality")
            .time(self.timestamp)
            .field("no2", self.no2)
            .field("no2_4m", self.no2_4m)
            .field("no", self.no)
            .field("no_4m", self.no_4m)
            .tag("unit", self.unit)
            .tag("street", self.street)
            .tag("station_id", self.station.station_id)
            .tag("lat", self.station.location.lat)
            .tag("lon", self.station.location.lon)
        )


@dataclass
class WeatherConditions:
    temperature: float
    precipitation: float
    wind_speed: float
    comment: str
    timestamp: datetime
    region: str


    def to_point(self) -> Point:
        return (
            Point("weather")
            .field("temperature", self.temperature)
            .tag("precipitation", self.precipitation)
            .tag("wind_speed", self.wind_speed)
            .tag("region", self.region)
            .tag("comment", self.comment)
            .time(self.timestamp)
        )
