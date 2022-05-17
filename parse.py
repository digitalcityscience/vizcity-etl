from dataclasses import dataclass
from typing import List

import jmespath


@dataclass
class EvChargingStationEvent:
    status: str
    address: str
    latitude: float
    longitude: float


def extract_ev_charging_events(json_data: str) -> List[EvChargingStationEvent]:
    result = jmespath.search(
        "value[*].{status:Datastreams[0].Observations[0].result, longitude: Locations[0].location.geometry.coordinates[0], latitude: Locations[0].location.geometry.coordinates[1], address:Locations[0].description}",
        json_data,
    )
    return result  # type: ignore
