from etl import (
    collect_e_charging_stations,
    collect_stadtrad,
    fetch_and_transform_geoportal_events,
)
from parse import extract_parking_usage

BUCKET = "cintra"


def collect_parking_usage():
    fetch_and_transform_geoportal_events(
        BUCKET,
        "https://geodienste.hamburg.de/HH_WFS_Verkehr_opendata?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typename=de.hh.up:verkehr_parkhaeuser",
        extract_parking_usage,
    )


def collect():
    collect_parking_usage()
    collect_stadtrad(BUCKET)
    collect_e_charging_stations(BUCKET)
