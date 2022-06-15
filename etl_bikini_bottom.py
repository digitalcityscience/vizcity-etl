from etl import collect_air_quality, collect_stadtrad, collect_swis

BUCKET = "BikiniBottom"


def collect():
    collect_stadtrad(BUCKET)
    collect_air_quality(BUCKET)
