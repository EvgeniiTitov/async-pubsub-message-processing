from app.utils import parse_arguments
from app import run


def main() -> int:
    run(**parse_arguments())
    return 0


if __name__ == "__main__":
    main()
