from util.mongo import get_connection_info, connect
import sys


def main_with_args(remote_mongo: str):
    config = get_connection_info(remote_mongo)
    with connect(**config) as connection:
        print(connection)


def main():
    n_args = len(sys.argv)
    main_with_args(sys.argv[n_args-1])


if __name__ == "__main__":
    main()
