from etl import (
    collect_air_quality,
    collect_airport_arrivals,
    collect_e_charging_stations,
    collect_parking_usage,
    collect_stadtrad,
)

BUCKET = "cintra"


def collect():
    collect_parking_usage(BUCKET)
    collect_stadtrad(BUCKET)
    collect_e_charging_stations(BUCKET)
    collect_air_quality(BUCKET)
    collect_airport_arrivals(BUCKET)
