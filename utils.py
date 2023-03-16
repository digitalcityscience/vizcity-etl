import re
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pyproj import Transformer
from geopy.geocoders import Nominatim
from retry import retry

geolocator = Nominatim(user_agent="dcs_vizcity-etl")

GERMANY_TIMEZONE = timezone(+timedelta(hours=2))


def parse_timestamp_like(timestamp_like: float) -> datetime:
    return datetime.strptime(
        str(round(float(timestamp_like), 6)), "%Y%m%d%H%M%S.%f"
    ).astimezone(GERMANY_TIMEZONE)


def parse_date_time(date: str, time: str) -> datetime:
    return datetime.strptime(
        f"{date}{time}",
        "%Y-%m-%d%H:%M:%S",
    ).astimezone(GERMANY_TIMEZONE)

def parse_day_time_relative(day_time) -> datetime:
    now = datetime.now().astimezone(GERMANY_TIMEZONE)
    year_month_day = now.strftime("%Y-%m-%d")
    return datetime.strptime(
        f"{year_month_day} {day_time}",
        "%Y-%m-%d %A %I:%M %p",
    ).astimezone(GERMANY_TIMEZONE)


def parse_date_time_without_seconds(date_time: str) -> datetime:
    return datetime.strptime(
        f"{date_time}",
        "%d.%m.%Y %H:%M",
    ).astimezone(GERMANY_TIMEZONE)


def parse_date_comma_time(date_comma_time: str) -> datetime:
    date, time = date_comma_time.split(", ")
    return datetime.strptime(
        f"{date}-{time}",
        "%d.%m.%Y-%H:%M",
    ).astimezone(GERMANY_TIMEZONE)


def parse_date_with_timezone_text(date_string: str) -> datetime:
    if date_string is None:
        return datetime.fromtimestamp(0)
    cleaned_date_string = re.split("(\[\w+(\/)?\w*\])", date_string)
    return datetime.strptime(
        cleaned_date_string[0],
        "%Y-%m-%dT%H:%M:%S.%f%z",
    )


def now_germany() -> datetime:
    return datetime.now().astimezone(GERMANY_TIMEZONE)


TransformerFromCRS = lru_cache(Transformer.from_crs)


def from_epsg25832_to_gps(x: float, y: float) -> dict[str,float]:
    transformer = TransformerFromCRS("EPSG:25832", "EPSG:4326")
    lat, lon = transformer.transform(x, y)
    return {"lat":round(lat, 7), "lon":round(lon, 7)}


def from_millisecond_timestamp(ts:float) -> datetime:
    try:
        result = datetime.fromtimestamp(ts)
    except ValueError:
        return from_millisecond_timestamp(ts / 1000)
    return result.astimezone(GERMANY_TIMEZONE)


def has_numbers(inputString):
        return any(char.isdigit() for char in inputString)


def get_location_info(lat:float, lon:float) -> str:
    if None in [lat, lon]:
        return "unknown", "unknown", "unknown"
    
    @retry(tries=5)
    def do_geocode(address, attempt=1, max_attempts=5):
        return geolocator.reverse("{}, {}".format(lat, lon)).address.split(",")

    
    name = do_geocode(lat, lon)

    if has_numbers(name[0]):
        # usually street number is first item
        return name[1], name[2], name[3]

    # but the order of the address is not always the same
    if not has_numbers(name[0]) and has_numbers(name[1]):
        # first element is a place name, like a shop
        return name[2], name[3], name[4]
    
    if not has_numbers(name[0]) and not has_numbers(name[1]):
        # does not contain a street number at all
        return name[0], name[1], name[2]
    
    
