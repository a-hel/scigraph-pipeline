import json
from logging import Logger
from pony import orm

logger = Logger()

def load_config(config_file="../config.json"):
    with open(config_file, 'r') as f:
        conf = json.load(f)
    return conf

def check(config_file="../config.json"):
    passed = True
    try:
        config = load_config(config_file=config_file)
    except FileNotFoundError:
        logger.error("Unable to find config file (%s)." % config_file)
        return False
    except json.JSONDecodeError as e:
        logger.error("Unable to parse config file: \n %s" % e)
        return False
    try:
        pg_config = config["postgres"]
    except KeyError:
        logger.error("Config for database not defined.")
        return False
    
