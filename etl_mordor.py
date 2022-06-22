from etl import collect_weather


BUCKET = "mordor"


def collect():
    collect_weather(53.535058, 9.876773, BUCKET)
