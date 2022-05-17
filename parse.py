from dataclasses import dataclass
from typing import List

import jmespath


@dataclass
class EvChargingStationEvent:
    status: str
    address: str
    lat: float
    lon: float
    timestamp: int


def extract_ev_charging_events(json_data: str) -> List[EvChargingStationEvent]:
    result = jmespath.search(
        "value[*].{status:Datastreams[0].Observations[0].result, lon: Locations[0].location.geometry.coordinates[0], lat: Locations[0].location.geometry.coordinates[1], address:Locations[0].description, timestamp:Datastreams[0].Observations[0].phenomenonTime}",
        json_data,
    )
    return result  # type: ignore
