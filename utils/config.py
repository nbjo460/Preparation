"""Configuration management module."""

import json
from pathlib import Path
from typing import Optional


class LoggerConfig:
    """Configuration for logger settings."""

    config: Optional[dict] = None
    flag_does_read_config: bool = False

    def __init__(self) -> None:
        """Initialize logger config from JSON file."""
        if not LoggerConfig.flag_does_read_config:
            config_path = Path(__file__).parent.parent / "configs" / "logger_config.json"
            config_data = json.loads(config_path.read_text())
            LoggerConfig.config = config_data
            LoggerConfig.flag_does_read_config = True

    @property
    def file_name(self):
        return LoggerConfig.config["file_name"]
    @property
    def logs_folder(self) -> Path:
        """Get path to logs folder."""
        return Path(__file__).parent.parent / "logs"
