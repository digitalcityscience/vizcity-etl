from etl import collect_detailed_air_quality_hamburg_list, collect_traffic_status

BUCKET = "Duckville"


def collect():
    collect_traffic_status(BUCKET)
    collect_detailed_air_quality_hamburg_list(BUCKET)
