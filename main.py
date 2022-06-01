import sys

from dotenv import load_dotenv


def collect_group1():
    load_dotenv()

    from etl_atlantis import collect as collect_atlantis
    from etl_cintra import collect as collect_cintra
    from etl_gotham import collect as collect_gotham

    collect_atlantis()
    collect_cintra()
    collect_gotham()


def collect_group2():
    load_dotenv(".env2")
    from etl_bikini_bottom import collect as collect_bikini_bottom
    from etl_duckville import collect as collect_duckville
    from etl_lazytown import collect as collect_lazytown
    from etl_oribos import collect as collect_oribos

    collect_duckville()
    collect_bikini_bottom()
    collect_lazytown()
    collect_oribos()


if __name__ == "__main__":
    group = 1
    try:
        group = int(sys.argv[1])
    except IndexError:
        print("No group number given! Will use the default group 1")

    print(f"Collecting data for group {group}")
    if group == 1:
        collect_group1()
    if group == 2:
        collect_group2()
