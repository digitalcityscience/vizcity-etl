from etl import collect_air_quality, collect_traffic_status


BUCKET = "Oribos"


def collect():
    collect_air_quality(BUCKET)
    collect_traffic_status(BUCKET)
