from ctypes import Union
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, Optional, List

from dataclasses_json import dataclass_json
from influxdb_client import Point

from utils import parse_date_with_timezone_text
import shapely.wkt
from shapely.geometry import LineString as ShapelyLineString


@dataclass_json
@dataclass
class AddressInfo:
   street_number: str
   street: str
   neighborhood: str
   district: str
   geom_id: str
   lane_count: Optional[int] = None
   max_velocity: Optional[int] = None


   @classmethod
   def from_dict(cls, dict):
        return AddressInfo(
            street_number=dict["street_number"],
            street=dict["street"],
            neighborhood=dict["neighborhood"],
            district=dict["district"],
            geom_id=dict["geom_id"],
            max_velocity=dict["jax_velocity"],
            lane_count=dict["lane_count"]
    )

   def get_address(self):
       return "%s %s" % (self.street, self.street_number)


@dataclass
class LocationPointEPSG:
    x: float
    y: float
    system: int = 25832

    @classmethod
    def from_single_line(cls, line: str = "0 0"):
        x, y = line.split()
        return LocationPointEPSG(float(x), float(y))


@dataclass
class LocationLineEPSG:
    points: List[LocationPointEPSG]
    shapely_linestring: ShapelyLineString

    @classmethod
    def from_gml_posList(cls, coords_as_string: str = "0 0 0 0"):
        # shapely does not read WKT strings with less then 3 points
        # https://gis.stackexchange.com/a/447579
        if len(coords_as_string.split()) < 5:
            coords = [float(val) for val in coords_as_string.split()]
            line: ShapelyLineString = ShapelyLineString(
                [
                    (coords[0], coords[1]),
                    (coords[2], coords[3])
                ]
            )
        else:
            # convert provided string (float float float float float float ...)
            # to (float float, float float, float float, ...)
            # as valid WKT string
            wkt_str = ""
            for idx, val in enumerate(coords_as_string.split()):
                wkt_str += val
                if idx != 0 \
                and not (idx % 2) == 0 \
                and not idx == len(coords_as_string.split()) - 1:
                    wkt_str += ", "

                wkt_str += " "
            
            wkt_str = "LINESTRING(%s)" % wkt_str
            line: ShapelyLineString = shapely.wkt.loads(wkt_str)

        points = [LocationPointEPSG(pt[0], pt[1]) for pt in line.coords]

        return LocationLineEPSG(
            points=points,
            shapely_linestring=line
        )
    

def add_location_EPSG_to_point(
    location_EPSG: Optional[LocationPointEPSG], point: Point
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
    location_EPSG: Optional[LocationPointEPSG]

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
    location_EPSG: Optional[LocationPointEPSG]

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
class TrafficStatus:
    timestamp: str
    status: str
    status_index_class: int
    street_class: str    
    street_center_lat: float
    street_center_lon: float
    street_direction: str
    address_info: AddressInfo

    def to_point(self) -> Point:

        point = (
            Point("traffic_status")
            .field("traffic_flow_index_class", self.status_index_class)
            .field("street_center_lat", self.street_center_lat)
            .field("street_center_lon", self.street_center_lon)
            .tag("street_segment_id", self.address_info.geom_id)
            .tag("street_class", self.street_class)
            .tag("street_district", self.address_info.district)
            .tag("street_neighborhood", self.address_info.neighborhood)
            .tag("address", self.address_info.get_address())
            .tag("street_direction", self.street_direction)
            .tag("lane_count", self.address_info.lane_count)
            .tag("max_velocity", self.address_info.max_velocity)
            .tag("traffic_flow_category", self.status)
            .time(self.timestamp)
        )

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
    location_EPSG: Optional[LocationPointEPSG]

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
class TrafficCounts(Location):
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
class BikeTrafficStatus(TrafficCounts):
    measurement_name = "fahrrad_verkehr"


@dataclass
class Parking(Location):
    name: str
    utilization: str
    free: int
    capacity: int
    price: str
    timestamp: Any
    location_EPSG: Optional[LocationPointEPSG]

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
class DWDWeatherStation:
    # https://www.dwd.de/DE/leistungen/met_verfahren_mosmix/mosmix_stationskatalog.cfg?view=nasPublication&nn=16102
    location: Location
    id: str
    name: str
    elevation: int


@dataclass
class WeatherConditions:
    temperature: float
    precipitation: float
    wind_speed: float
    timestamp: datetime
    station: DWDWeatherStation
    humidity: int
    pressure: int

    def to_point(self) -> Point:
        return (
            Point("weather")
            .field("temperature", self.temperature)
            .tag("precipitation", self.precipitation)
            .tag("wind_speed", self.wind_speed)
            .tag("humidity", self.humidity)
            .tag("pressure", self.pressure)
            .tag("region", self.station.name)
            .tag("station_elevation", self.station.elevation)
            .tag("station_id", self.station.id)
            .tag("lat",self.station.location.lat)
            .tag("lon",self.station.location.lon)
            .time(self.timestamp)
        )
