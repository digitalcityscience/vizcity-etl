from etl import collect_e_charging_stations, collect_parking_usage

BUCKET = "LazyTown"


def collect():
    collect_e_charging_stations(BUCKET)
    collect_parking_usage(BUCKET)
