import logging
from datetime import datetime


def PipelineLogger(name=None):
    logger = logging.Logger(name=name or __name__)

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console.setFormatter(console_formatter)

    file = logging.FileHandler(filename=f"logs/{datetime.now().strftime('%Y-%m-%d')}.log", mode="a")
    file.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s; %(name)s; %(module)s; %(levelname)s; "%(message)s"')
    file.setFormatter(file_formatter)

    logger.addHandler(console)
    logger.addHandler(file)
    return logger