from etl import collect_stadtrad, collect_swis, collect_weather_dwd
from models import DWDWeatherStation, Location


BUCKET = "gotham"


def collect():
    collect_stadtrad(BUCKET)
    collect_swis(BUCKET)
    dwd_hamburg = DWDWeatherStation(
        location=Location(lat=53.38, lon=10.00),
        id="10147",
        name="HAMBURG-FU",
        elevation=16,
    )
    collect_weather_dwd(dwd_hamburg, BUCKET)