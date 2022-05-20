import requests

from influxdb import InfluxPayload, write_to_influx
from parse import extract_ev_charging_events, extract_parking_usage_lazytown

print("Collecting remote data...")
r = requests.get(
    "https://geodienste.hamburg.de/HH_WFS_Verkehr_opendata?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typename=de.hh.up:verkehr_parkhaeuser"
)
result_json = r.text
print("Parsing events...")
events = extract_parking_usage_lazytown(result_json)
print(events)
print("Writing to timeseries db...")
write_to_influx(
    InfluxPayload(
        bucket="LazyTown",
        record=events,
        measurment_name="parking-spaces",
        tag_keys=[
            "name",
            "free",
            "capacity",
            "price",
            "lat",
            "lon",
        ],
        time_key="timestamp",
        field_keys=["utilization"],
    )
)
