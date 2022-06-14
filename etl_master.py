from etl import (
    collect_air_quality,
    collect_airport_arrivals,
    collect_bike_traffic_status,
    collect_detailed_air_quality_hamburg_list,
    collect_e_charging_stations,
    collect_parking_usage,
    collect_stadtrad,
    collect_swis,
    collect_traffic_status,
)

BUCKET = "vizcity-master"


def collect():
    collect_swis(BUCKET)
    collect_bike_traffic_status(BUCKET)
    collect_parking_usage(BUCKET)
    collect_stadtrad(BUCKET)
    collect_e_charging_stations(BUCKET)
    collect_airport_arrivals(BUCKET)
    collect_detailed_air_quality_hamburg_list(BUCKET)
    collect_air_quality(BUCKET)
    collect_traffic_status(BUCKET)
