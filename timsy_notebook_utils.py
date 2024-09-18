import configparser
import functools
import logging
import os
from pathlib import Path
from typing import Union

from sqlalchemy import create_engine, Engine, text, URL
from sqlalchemy.orm import sessionmaker
import pandas as pd

basedir = os.path.abspath(os.path.dirname(__name__))
logger = logging.getLogger(__name__)

def build_url_from_config() -> URL:
    config = configparser.ConfigParser()
    config.read('config.ini')
    db_config = config['database']
    url = URL.create(
        drivername=db_config['drivername'],
        host=db_config['host'],
        database=db_config['database'],
        query={'driver': db_config['driver'], 'trusted_connection': db_config['trusted_connection']}
    )
    return url

class SqlUtil:
    _url: URL = build_url_from_config()
    _engine: Engine = None
    _session: sessionmaker = None

    def __new__(cls, *args, **kwargs):
        raise TypeError("This class cannot be instantiated")

    @classmethod
    def url(cls):
        if not cls._url:
            cls._url = build_url_from_config()
        return cls._url

    @classmethod
    def engine(cls) -> Engine:
        if not cls._engine:
            cls._engine = create_engine(cls.url())
        return cls._engine

    @classmethod
    def session(cls):
        if not cls._session:
            cls._session = sessionmaker(bind=cls.engine())
        return cls._session


def inject_engine(function):
    # Connect to your database
    engine: Engine = SqlUtil.engine()

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        return function(*args, **kwargs, engine=engine)
    return wrapper

@inject_engine
def sql_query_to_dataframe(query: str, engine: Engine = None) -> pd.DataFrame:
    df = pd.read_sql_query(query, engine)
    return df

@inject_engine
def sql_compiled_query_to_dataframe(query: text, engine: Engine = None) -> pd.DataFrame:
    df = pd.read_sql_query(query, engine)
    return df

def sql_read(file_path: Union[ str | Path]) -> str:
    with open(file_path, 'r') as file:
        sql_query = file.read()
    return sql_query

@inject_engine
def parse_query_params(input_query:str, engine:Engine = None):
    query = text(input_query)
    compiled_pre_query = query.compile(engine)
    return compiled_pre_query.params.keys()

@inject_engine
def parse_query_parameters(input_query:str, engine:Engine = None):
    query = text(input_query)
    compiled_pre_query = query.compile(engine)
    parameters = compiled_pre_query.params
    return parameters


def parse_sql_file_params(file_path: Union[str | Path]):
    query = sql_read(file_path)
    return parse_query_params(query)





def get_basedir():
    return basedir


def get_folder(*folder_name: str | list[str]) -> Path:
    folder_path = Path(basedir, *folder_name)
    folder_path.mkdir(parents=True, exist_ok=True)
    return folder_path


def get_project_folder(folder_name:str) -> Path:
    folder_path = check_for_folder_recursive(folder_name, basedir)
    if folder_path is None:
        folder_str_path = Path(basedir, folder_name)
        os.makedirs(folder_str_path)
        folder_path = Path(folder_str_path)
    return folder_path

def check_for_folder_recursive(folder_name:str, current_dir) -> Path | None:
    folder_str_path = os.path.join(current_dir, folder_name)
    if not os.path.exists(folder_str_path):
        folders = [f for f in os.listdir(current_dir) if os.path.isdir(f)]
        while len(folders) > 0:
            folder = folders.pop()
            next_dir = os.path.join(current_dir, folder)
            logger.debug("Checking:", next_dir)
            if is_library_root(next_dir):
                logger.debug("Found root:", next_dir)
                continue
            folder_path = check_for_folder_recursive(folder_name, next_dir)
            if folder_path is not None:
                logger.debug("Found:", folder_path)
                return folder_path
    else:
        return Path(folder_str_path)
    return None


def is_library_root(path: str | Path) -> bool:
    if not isinstance(path, Path):
        path = Path(path)
    if path.is_dir():
        if 'pyvenv.cfg' in os.listdir(path):
            return True
    return False

def get_sql_scripts():
    sql_script_dir = get_project_folder('sql_scripts')
    sql_scripts = [f for f in os.listdir(sql_script_dir) if f.endswith('.sql')]
    sql_scripts_dict = {}
    for sql_script in sql_scripts:
        script_name = sql_script.split('.')[0]
        sql_scripts_dict[script_name] = os.path.join(sql_script_dir, sql_script)
    return sql_scripts_dict
