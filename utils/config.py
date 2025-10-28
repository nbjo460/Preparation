import json
from pathlib import Path


class MapConfig:
    pass
class LoggerConfig:
    config: dict = None
    flag_does_read_config = False
    def __init__(self):
        if not LoggerConfig.flag_does_read_config:
            config_path = Path(__file__).parent.parent / "configs" / "logger_config.json"
            config_data = json.loads(config_path.read_text())

            LoggerConfig.config = config_data

            LoggerConfig.flag_does_read_config = True

    @property

    def file_name(self):
        return LoggerConfig.config["file_name"]


# print(LoggerConfig().file_name)

