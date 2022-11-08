import logging
from datetime import datetime
from typing import Union


def PipelineLogger(name: Union[str, bool, None] = None) -> logging.Logger:
    console_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_format = '%(asctime)s; %(name)s; %(module)s; %(levelname)s; "%(message)s"'

    logger = logging.Logger(name=name or __name__)

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter(console_format)
    console.setFormatter(console_formatter)

    file = logging.FileHandler(
        filename=f"logs/{datetime.now().strftime('%Y-%m-%d')}.log", mode="a"
    )
    file.setLevel(logging.INFO)
    file_formatter = logging.Formatter(file_format)
    file.setFormatter(file_formatter)

    logger.addHandler(console)
    logger.addHandler(file)
    return logger
