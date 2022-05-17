from dataclasses import dataclass
import json
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
        "value[*].{status:Datastreams[0].Observations[0].result, latitude: Locations[0].location.geometry[0], address:Locations[0].description}",
        json_data,
    )
    print(json.dumps(result))
    return result  # type: ignore
    
