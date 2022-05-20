from dataclasses import dataclass
from datetime import datetime

from influxdb_client import Point


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

    def to_point(self) -> Point:
        return (
            Point("stadtrad_station")
            .tag("name", self.name)
            .tag("count_pedelec", self.count_pedelec)
            .tag("count_bike", self.count_bike)
            .tag("count_cargobike_electric", self.count_cargobike_electric)
            .tag("lat", self.lat)
            .tag("lon", self.lon)
            .field("count", self.count)
            .time(self.timestamp)
        )
