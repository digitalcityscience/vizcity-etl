import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, Point

from dotenv import load_dotenv
load_dotenv()
 
BUCKET_GROUP_LUT = {
    "atlantis": "GROUP1",
    "mordor": "GROUP1",
    "cintra": "GROUP1",
    "gotham": "GROUP1",
    "BikiniBottom": "GROUP2",
    "Duckville": "GROUP2",
    "LazyTown": "GROUP2",
    "Oribos": "GROUP2",
    "vizcity-master": "MASTER",
}


def get_current_influx_config(group_name: str = "master") -> Dict[str, Optional[str]]:
    return {
        "url": os.getenv("INFLUX_URL", ""),
        "token": os.getenv(f"INFLUX_TOKEN_{group_name.upper()}"),
        "org": os.getenv(f"INFLUX_ORG_{group_name.upper()}"),
    }


@dataclass
class InfluxPayload:
    record: List[Any]
    bucket: str
    measurment_name: str
    tag_keys: List[str]
    time_key: str
    field_keys: List[str]


def write_points_to_influx(bucket: str, points: List[Point]):
    group = BUCKET_GROUP_LUT.get(bucket, "")
    with InfluxDBClient(**get_current_influx_config(group)) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket, record=points)


def write_to_influx(payload: InfluxPayload):
    group = BUCKET_GROUP_LUT.get(payload.bucket, "")
    with InfluxDBClient(**get_current_influx_config(group)) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(
            bucket=payload.bucket,
            record=payload.record,
            record_measurement_name=payload.measurment_name,
            record_tag_keys=payload.tag_keys,
            record_time_key=payload.time_key,
            record_field_keys=payload.field_keys,
        )
