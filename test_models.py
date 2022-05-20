from datetime import datetime
from models import StadtradStation
from influxdb_client import Point


def test_stadtrad_station_to_point():
    given = StadtradStation(
        name="Enckeplatz / Hütten",
        count=0,
        count_pedelec=0,
        count_bike=0,
        count_cargobike_electric=0,
        lat=564722.695,
        lon=5934167.221,
        timestamp=datetime.now()
    )
    expected = (
        Point("stadtrad_station")
        .tag("name", given.name)
        .tag("count_pedelec", given.count_pedelec)
        .tag("count_bike", given.count_bike)
        .tag("count_cargobike_electric", given.count_cargobike_electric)
        .tag("lat", given.lat)
        .tag("lon", given.lon)
        .field("count", given.count)
        .time(given.timestamp)
    )
    assert expected.to_line_protocol() == given.to_point().to_line_protocol()
