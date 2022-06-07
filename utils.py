import re
from datetime import datetime, timedelta, timezone

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


def now_germany() ->datetime:
    return datetime.now().astimezone(GERMANY_TIMEZONE)