import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
from utils.config import LoggerConfig


class AppLogger:
    _initialized_loggers = set()

    def __init__(self, name: str = None):
        log_filename: str = LoggerConfig().file_name
        logs_dir : Path = LoggerConfig().logs_folder
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_file = logs_dir / log_filename
        if not log_file.exists():
            log_file.touch()



        if not name:
            name = __name__

        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False  # לא שולח הודעות למעלה

        if name not in AppLogger._initialized_loggers:
            # למסוף
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] [%(name)s:%(lineno)d] - %(message)s",
                datefmt="%H:%M:%S"
            )
            console_handler.setFormatter(console_formatter)
            console_handler.setLevel(logging.DEBUG)

            # לקובץ
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=2 * 1024 * 1024,  # 5MB
                backupCount=3,
                encoding="utf-8",
                delay=True
            )
            file_formatter = logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] [%(name)s:%(lineno)d] - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(file_formatter)

            self.logger.addHandler(console_handler)
            self.logger.addHandler(file_handler)

            AppLogger._initialized_loggers.add(name)

            if __name__  == name:
                self.logger.info("The app run...")

    def info(self, msg: str):
        self.logger.info(msg)

    def warning(self, msg: str):
        self.logger.warning(msg)

    def error(self, msg: str):
        self.logger.error(msg)

    def debug(self, msg: str):
        self.logger.debug(msg)
