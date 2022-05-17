import requests

from influxdb import InfluxPayload, write_to_influx
from parse import extract_ev_charging_events

print("Collecting remote data...")
r = requests.get(
    "https://iot.hamburg.de/v1.0/Things?$filter=Datastreams/properties/serviceName eq  'HH_STA_E-Ladestationen'&$count=true&$expand=Locations,Datastreams($expand=Observations($top=1;$orderby=phenomenonTime desc))"
)
result_json = r.json()
print(result_json["@iot.count"])
print("Parsing events...")
events = extract_ev_charging_events(result_json)
print(events)
print("Writing to timeseries db...")
write_to_influx(
    InfluxPayload(
        bucket="test-bucket",
        record=events,
        measurment_name="ev-charging-stations",
        tag_keys=[
            "address",
            "lat",
            "lon",
            "timestamp",
        ],
        time_key="timestamp",
        field_keys=["status"],
    )
)
