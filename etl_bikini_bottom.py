from etl import collect_stadtrad, collect_swis

BUCKET = "BikiniBottom"


def collect():
    collect_stadtrad(BUCKET)
    collect_swis(BUCKET)
