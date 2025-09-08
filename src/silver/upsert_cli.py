from src.core.log import configure_logging


def main(version: str):
    configure_logging(True)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--version", required=True)
    a = p.parse_args()
    main(a.version)
