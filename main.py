from dotenv import load_dotenv

load_dotenv()


def collect_group1():

    from etl_atlantis import collect as collect_atlantis
    from etl_cintra import collect as collect_cintra
    from etl_gotham import collect as collect_gotham

    collect_atlantis()
    collect_cintra()
    collect_gotham()


def collect_group2():
    from etl_bikini_bottom import collect as collect_bikini_bottom
    from etl_duckville import collect as collect_duckville
    from etl_lazytown import collect as collect_lazytown
    from etl_oribos import collect as collect_oribos

    collect_duckville()
    collect_bikini_bottom()
    collect_lazytown()
    collect_oribos()


def collect_master():
    from etl_master import collect as collect_master

    collect_master()


if __name__ == "__main__":
    collect_master()
    collect_group1()
    collect_group2()
