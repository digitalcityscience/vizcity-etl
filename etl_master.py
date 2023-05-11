from etl import (
    collect_air_quality,
    collect_airport_arrivals,
    collect_bike_traffic_status,
    collect_detailed_air_quality_hamburg_list,
    collect_e_charging_stations,
    collect_parking_usage,
    collect_stadtrad,
    collect_swis,
    collect_traffic_counts,
    collect_traffic_status,
    collect_weather_dwd,
)

from models import DWDWeatherStation, Location

BUCKET = "vizcity-master"


def collect():
    collect_bike_traffic_status(BUCKET)
    collect_traffic_counts(BUCKET)
    collect_traffic_status(BUCKET)
    collect_swis(BUCKET)
    collect_parking_usage(BUCKET)
    collect_e_charging_stations(BUCKET)
    collect_airport_arrivals(BUCKET)
    collect_detailed_air_quality_hamburg_list(BUCKET)
    collect_air_quality(BUCKET)
    dwd_hamburg = DWDWeatherStation(
        location=Location(lat=53.38, lon=10.00),
        id="10147",
        name="HAMBURG-FU",
        elevation=16,
    )
    collect_weather_dwd(dwd_hamburg, BUCKET)
    collect_stadtrad(BUCKET)

collect()