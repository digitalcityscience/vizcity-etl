import os
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

INFLUX_CONFIG = {
    "url": os.getenv("INFLUX_URL"),
    "token": os.getenv("INFLUX_TOKEN"),
    "org": os.getenv("INFLUX_ORG"),
}


@dataclass
class InfluxPayload:
    record: List[Any]
    bucket: str
    measurment_name: str
    tag_keys: List[str]
    time_key: str
    field_keys: List[str]


def write_to_influx(payload: InfluxPayload):
    with InfluxDBClient(**INFLUX_CONFIG) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(
            bucket=payload.bucket,
            record=payload.record,
            record_measurement_name=payload.measurment_name,
            record_tag_keys=payload.tag_keys,
            record_time_key=payload.time_key,
            record_field_keys=payload.field_keys,
        )