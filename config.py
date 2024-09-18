import configparser
from pathlib import Path

import logging

CONFIG_FILE = 'config.ini'
# module_logger = logging.getLogger(f'MainAppLogger.{__name__}')
module_logger = logging.getLogger(__name__)


def config_error_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError as ke:
            module_logger.warning(f'{type(ke).__name__}: {ke}')
            return None
        except configparser.NoOptionError as noe:
            module_logger.warning(f'{type(noe).__name__}: {noe}')
            return None
        except FileNotFoundError as fnfe:
            module_logger.error(f'{type(fnfe).__name__}: {fnfe}')
            raise fnfe
        except Exception as e:
            module_logger.warning(f'Unhandled Exception - {type(e).__name__}: {e}')
            raise e

    return wrapper


def override_default_config_file(config_file: str):
    global CONFIG_FILE
    CONFIG_FILE = config_file


def reset_default_config_file():
    """ Reset the default hard coded config.ini. """
    global CONFIG_FILE
    cp = configparser.ConfigParser()
    cp.default_section = 'DEFAULT'
    cp.set('DEFAULT', 'insertDefaultHere', 'TODO')
    cp.set('DEFAULT', 'loggerName', 'TimsyAppLogger')
    cp.set('DEFAULT', 'loggerFileName', 'logs/timsy_app.log')
    cp.set('DEFAULT', 'testIndexNumber', '86')
    with open(CONFIG_FILE, 'w') as f:
        cp.write(f)


@config_error_handler
def check_config_file(config_file: str = CONFIG_FILE):
    # Check if the config file exists
    if not Path(config_file).exists():
        raise FileNotFoundError(f"Config file '{config_file}' not found.")
    # Check reader can open config file
    with open(config_file, 'r') as f:
        return True


class Config:
    def __init__(self, config_file: str = CONFIG_FILE):
        try:
            check_config_file(config_file)
        except FileNotFoundError as fnfe:
            reset_default_config_file()
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        self.logger = module_logger

    @config_error_handler
    def get(self, section='DEFAULT', key=''):
        module_logger.info(f'Getting {key} from {section}')
        return self.config[section][key]

    @config_error_handler
    def get_int(self, section='DEFAULT', key=''):
        return self.config.getint(section, key)

    @config_error_handler
    def get_float(self, section='DEFAULT', key=''):
        return self.config.getfloat(section, key)

    @config_error_handler
    def get_boolean(self, section='DEFAULT', key=''):
        return self.config.getboolean(section, key)

    @config_error_handler
    def get_list(self, section='DEFAULT', key=''):
        return self.config[section][key].split(',')

    @config_error_handler
    def get_section(self, section='DEFAULT'):
        return dict(self.config[section])

    @config_error_handler
    def get_sections(self):
        return self.config.sections()


if __name__ == '__main__':
    config = Config(config_file='../config.ini')
    print(config.get('DEFAULT', 'applicationIcon'))
    print(config.get('DEFAULT', 'sqlFolder'))
    print(config.get_int('DEFAULT', 'sqlCount'))
    print(config.get_float('DEFAULT', 'sqlFloat'))
    print(config.get_boolean('DEFAULT', 'sqlBoolean'))
    print(config.get_list('DEFAULT', 'emptyList'))
    print(config.get_list('DEFAULT', 'applicationIcon'))
    print(config.get_section('EMPTY'))
    print(config.get_sections())
