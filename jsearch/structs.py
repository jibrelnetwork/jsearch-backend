from typing import NamedTuple


class AppConfig(NamedTuple):
    log_level: str
    no_json_formatter: bool
