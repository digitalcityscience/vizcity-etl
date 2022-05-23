from datetime import datetime

from influxdb_client import Point

from models import StadtradStation


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
        .field("count_pedelec", given.count_pedelec)
        .field("count_bike", given.count_bike)
        .field("count_cargobike_electric", given.count_cargobike_electric)
        .field("count", given.count)
        .tag("lat", given.lat)
        .tag("lon", given.lon)
        .time(given.timestamp)
    )
    assert expected.to_line_protocol() == given.to_point().to_line_protocol()
