import mysql.connector
from mysql.connector import connect, Error, errorcode, pooling
import logging
import os

from ..enums import *

_connection_pool = None


def get_connection(config=None):
    global _connection_pool

    if config is None:
        config = {
            env_database_host: os.getenv(env_database_host),
            env_database_username: os.getenv(env_database_username),
            env_database_password: os.getenv(env_database_password)
        }
    if _connection_pool is None:
        try:
            _connection_pool = pooling.MySQLConnectionPool(
                host=config[env_database_host],
                user=config[env_database_username],
                password=config[env_database_password],
                database="capstone",
                pool_name="capstone_pool")
        except Error as e:
            logging.exception(e)
    try:
        connection = _connection_pool.get_connection()
        return connection
    except Error as e:
        logging.exception(e)
        return None


def close_connection_or_cursor(connection_cursor):
    if connection_cursor is not None:
        connection_cursor.close()
