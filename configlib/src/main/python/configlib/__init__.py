import logging
import os


logger = logging.getLogger(__name__)

_config = None
_config_file = os.getenv("CONFIG_FILE")


def set_config_file(config_file):
    global _config_file
    _config_file = config_file


def get_config():
    global _config
    if _config is None:
        if os.path.exists(_config_file):
            _config = eval(open(_config_file).read())

    return _config


def get_config_key(key, default=None):
    eval = os.getenv(key, None)
    cval = get_config().get(key)
    return eval or cval or default
