import json
import datetime
from typing import List
from etl import load_events
from models import AirQuality, DWDWeatherStation, Location, LocationPointEPSG, StadtradStation, TrafficCounts, BikeTrafficStatus, WeatherConditions
import jmespath


BEGINNING = datetime.datetime.fromisoformat("2023-03-23T13:29:00")
END = datetime.datetime.fromisoformat("2023-03-24T10:12")

def extract_bike_traffic_status() -> List[TrafficCounts]:
    with open("lost_data/bike_counts.json", "r") as f:
        json_data = json.load(f)


    results = jmespath.search(
        "value[*].{counted_traffic:Datastreams[0].Observations[0].result, lon: Datastreams[0].observedArea.coordinates[0], lat: Datastreams[0].observedArea.coordinates[1], timestamp:Datastreams[0].Observations[0].resultTime, station_id:name}",
        json_data,
    )

    results = list(map(lambda result: BikeTrafficStatus(**result), results))  # type: ignore

    results = list(filter(lambda result: BEGINNING <= datetime.datetime.fromisoformat(result.timestamp[:-1]) <= END, results))

    return results



def extract_traffic_counts(json_data: str) -> List[TrafficCounts]:
    with open("lost_data/traffic_counts.json", "r") as f:
        json_data = json.load(f)

    results = jmespath.search(
        "value[*].{counted_traffic:Datastreams[0].Observations[0].result, lon: Datastreams[0].observedArea.coordinates[0], lat: Datastreams[0].observedArea.coordinates[1], timestamp:Datastreams[0].Observations[0].resultTime, station_id:properties.assetID}",
        json_data,
    )



    results = list(map(lambda result: TrafficCounts(**result), results))  # type: ignore
    results = list(filter(lambda result: BEGINNING <= result["timestamp"] <= END, results))

    return results



def extract_weather():
    # read weather csv
    with open("lost_data/weather.csv", "r") as f:
        lines = f.readlines()
    
    # parse csv
    weather_data = []
    for line in lines[1:]:
        line = line.strip()
        line = line.split(",")
        weather_data.append(
            WeatherConditions(
                precipitation=float(line[4]),
                temperature=int(line[1]),
                pressure=line[9],
                wind_speed=line[7],
                timestamp=datetime.datetime.fromisoformat(line[0]),
                humidity=line[3],
                station=DWDWeatherStation(
                    location=Location(lat=53.38, lon=10.00),
                    id="10147",
                    name="HAMBURG-FU",
                    elevation=16,
                )
            ))
        
    return weather_data


def reproject(lat, lon, source, target):
    import pyproj

    source = pyproj.Proj(init=f'epsg:{source}')
    target = pyproj.Proj(init=f'epsg:{target}')

    x, y = pyproj.transform(source, target, lon, lat)
    return x, y


def get_stadtrad_data():
    import xmltodict
    import os

    with open("lost_data/stadtrad.json", "r") as f:
        json_data = json.load(f)

    fixture_file = os.path.join(
        os.path.dirname(__file__), "lost_data", "HH_WFS_Stadtrad_stations.gml"
    )


    with open(fixture_file) as xml_file:
        xml_data = xml_file.read()

    xml = xmltodict.parse(
        xml_data, process_namespaces=False
    )

    station_names = {
        "acc438d5-87f7-49cb-a2f7-4609f772190b": "Lohbrügger Markt / Ludwig-Rosenberg-Ring",
        "a52119bd-a741-4078-baf8-80841e1edd48": "U Langenhorn Markt / Tangstedter Landstraße",
        "b0242d4b-c334-48ce-8f1c-1bdd66736819": "S Wellingsbüttel / Rolfinckstraße",
        "fb5a5361-6a5b-48fd-a404-2e0d91b1ae22": "Max-Zelck-Straße / Haus der Kirche",
        "a980a698-4d3b-46e6-b64d-2d136dcc804c": "Heidhorst / Bockhorster Weg",
        "265b55fb-30d7-4314-aeb5-03640faa89e2": "U Oldenfelde / Busbrookhöhe",
        "2da9f852-ae9f-444e-8e13-7d0b9aadf18a": "Schellerdamm / Hausnummer 22",
        "b4c420e6-5743-402f-a5a6-382a22288c5e": "Billwerder Billdeich / TSG Sportforum",
        "e1f26658-74cf-47cb-aa38-b0934ee77a6f": "Lohbrügger Landstraße / BG Klinikum",
        "9980098f-2acf-4f8d-90ca-411abbc1459a": "Henriette-Herz-Ring / Grachtenplatz", 
        "d65fcfd2-e98e-44b6-9047-8a6512242cf1": ""
    }

    entries = [
        entry["de.hh.up:stadtrad_stationen"]
        for entry in xml["wfs:FeatureCollection"]["wfs:member"]
    ]

    for entry in entries:
        station_names[entry["de.hh.up:uid"]] = entry["de.hh.up:name"]

    data = []


    for entry in json_data["value"]:
        station_id = entry["name"].replace("Fahrräder an StadtRad-Station ", "")
        try:
            lat=entry["observedArea"]["coordinates"][1][1]
            lon=entry["observedArea"]["coordinates"][1][0]
            x,y = entry["observedArea"]["coordinates"][0][0], entry["observedArea"]["coordinates"][0][1]
        except:
            lat=entry["observedArea"]["coordinates"][1]
            lon=entry["observedArea"]["coordinates"][0]
            x,y = reproject(lat, lon, 4326, 25832)
        
        name=station_names.get(station_id, "unknown")

        location_EPSG=LocationPointEPSG(x,y)

        for observation in entry["Observations"]:
            if BEGINNING <= datetime.datetime.fromisoformat(observation["phenomenonTime"][:-1]) <= END:
                data.append(StadtradStation( 
                    name = name,
                    lat = lat,
                    lon = lon,
                    location_EPSG = location_EPSG,
                    count=observation["result"],
                    count_bike=observation["result"],
                    count_pedelec=0,
                    count_cargobike_electric=0,
                    timestamp=datetime.datetime.fromisoformat(observation["phenomenonTime"][:-1]),
            ))
                
    return data



if __name__ == "__main__":

    # bike counts
    bikes_events = extract_bike_traffic_status()
    for bucket in ["vizcity-master", "atlantis"]:
        load_events(bucket, bikes_events)
        print("bike", bucket)

    # traffic counts
    cars_events = extract_bike_traffic_status()
    for bucket in ["vizcity-master", "atlantis", "Duckville", "Oribos"]:
        load_events(bucket, cars_events)
        print("traffic count", bucket)
    # weather
    for bucket in ["vizcity-master", "atlantis", "modor"]:
        events = extract_weather()
        for event in events:
            print(event.to_point().to_line_protocol())
            load_events(bucket, [event])
        print("weather", bucket) 

    # stadtrad 
    for bucket in ["vizcity-master", "atlantis", "BikiniBottom", "cintra", "gotham"]:
        events = get_stadtrad_data()
        load_events(bucket, events)
        print("stadtrad", bucket)

    