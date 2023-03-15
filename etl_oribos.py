from etl import (
    collect_air_quality,
    collect_detailed_air_quality_hamburg_list,
    collect_traffic_counts,
)

BUCKET = "Oribos"


def collect():
    collect_air_quality(BUCKET)
    collect_traffic_counts(BUCKET)
    collect_detailed_air_quality_hamburg_list(BUCKET)
