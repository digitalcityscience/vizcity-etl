from etl import collect_traffic_status

BUCKET = "Duckville"


def collect():
    collect_traffic_status(BUCKET)
