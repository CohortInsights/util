from util.io import get_config
from util.mongo import connect
import sys


def main_with_args(remote_mongo: str):
    global_config = get_config()
    config = global_config.get(remote_mongo)
    if not config:
        raise IOError("No key exists in configuration for '" + remote_mongo + "'")
    with connect(**config) as connection:
        print(connection)


def main():
    n_args = len(sys.argv)
    main_with_args(sys.argv[n_args-1])


if __name__ == "__main__":
    main()
