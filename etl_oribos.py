from etl import collect_air_quality


BUCKET = "Oribos"


def collect():
    collect_air_quality(BUCKET)
