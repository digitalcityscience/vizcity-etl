from etl import (
    collect_air_quality,
    collect_stadtrad,
    collect_swis,
    collect_traffic_status,
    collect_bike_traffic_status,
)

BUCKET = "atlantis"


def collect():
    collect_traffic_status(BUCKET)
    collect_bike_traffic_status(BUCKET)
    collect_stadtrad(BUCKET)
    collect_swis(BUCKET)
    collect_air_quality(BUCKET)
