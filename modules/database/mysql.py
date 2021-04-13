import mysql.connector
from mysql.connector import connect, Error, errorcode, pooling
import logging
import os

from modules.enums import *
from .housing_types import insert_housing_types

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
            # check to ensure tables exist
            create_application_tables()
            # populate tables as needed
            insert_housing_types()
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


def create_application_tables():
    connection = get_connection()
    cursor = connection.cursor()
    tables = {}
    tables[table_housing_type] = (
        "CREATE TABLE `{table_name}` ("
        "  `{column_housing_type_id}` int NOT NULL AUTO_INCREMENT,"
        "  `{column_housing_type}` varchar(255) NOT NULL UNIQUE,"
        "  PRIMARY KEY (`{column_housing_type_id}`)"
        ") ENGINE=InnoDB").format(
        table_name=table_housing_type,
        column_housing_type_id=column_housing_type_id,
        column_housing_type=column_housing_type
    )
    tables[table_date_zhvi] = (
        "CREATE TABLE `{table_name}` ("
        "  `date_zhvi_id` int NOT NULL AUTO_INCREMENT,"
        "  `{column_location_id}` int NOT NULL,"
        "  `{column_date}` datetime NOT NULL,"
        "  `{column_zhvi}` int NOT NULL,"
        "  PRIMARY KEY (`date_zhvi_id`),"
        "  UNIQUE KEY (`{column_date}`,`{column_location_id}`), KEY `location_id` (`{column_location_id}`),"
        "  FOREIGN KEY (`{column_location_id}`) "
        "  REFERENCES `{table_locations}` (`{column_location_id}`) ON DELETE CASCADE"
        ") ENGINE=InnoDB").format(
        table_name=table_date_zhvi,
        table_locations=table_locations,
        column_location_id=column_location_id,
        column_zhvi=column_zhvi,
        column_date=column_date
    )
    tables[table_locations] = (
        "CREATE TABLE `{table_name}` ("
        "  `{column_location_id}` int NOT NULL AUTO_INCREMENT,"
        "  `{housing_type_id}` int NOT NULL,"
        "  `{column_region_name}` int NOT NULL,"
        "  `{column_state}` varchar(2) NOT NULL,"
        "  `{column_city}` varchar(255) NOT NULL,"
        "  `metro` varchar(255) NOT NULL,"
        "  `county` varchar(255) NOT NULL,"
        "  PRIMARY KEY (`{column_location_id}`),"
        "  UNIQUE KEY (`{column_region_name}`,`{housing_type_id}`), KEY `housing_type_id` (`housing_type_id`),"
        "  FOREIGN KEY (`{housing_type_id}`) "
        "  REFERENCES `{housing_type_table}` (`{housing_type_id}`) ON DELETE CASCADE"
        ") ENGINE=InnoDB").format(
        table_name=table_locations,
        housing_type_table=table_housing_type,
        column_location_id=column_location_id,
        housing_type_id=column_housing_type_id,
        column_region_name=column_region_name,
        column_state=column_state,
        column_city=column_city
    )
    tables[table_calculations] = (
        "CREATE TABLE `{table_name}` ("
        "  `calculation_id` int NOT NULL AUTO_INCREMENT,"
        "  `{column_location_id}` int NOT NULL,"
        "  `{column_zhvi_start}` int NOT NULL,"
        "  `{column_zhvi_end}` int NOT NULL,"
        "  `{column_zhvi_min}` int NOT NULL,"
        "  `{column_zhvi_max}` int NOT NULL,"
        "  `{column_date_start}` datetime NOT NULL,"
        "  `{column_date_end}` datetime NOT NULL,"
        "  `{column_date_difference}` double NOT NULL,"
        "  `{column_zhvi_percent_change}` double NOT NULL,"
        "  PRIMARY KEY (`calculation_id`),"
        "  UNIQUE KEY (`{column_location_id}`), KEY `column_location_id` (`{column_location_id}`),"
        "  FOREIGN KEY (`{column_location_id}`)"
        "  REFERENCES `{location_table}` (`{column_location_id}`) ON DELETE CASCADE"
        ") ENGINE=InnoDB").format(
        table_name=table_calculations,
        location_table=table_locations,
        column_location_id=column_location_id,
        column_zhvi_start=column_zhvi_start,
        column_zhvi_end=column_zhvi_end,
        column_zhvi_min=column_zhvi_min,
        column_zhvi_max=column_zhvi_max,
        column_date_start=column_date_start,
        column_date_end=column_date_end,
        column_date_difference=column_date_difference,
        column_zhvi_percent_change=column_zhvi_percent_change
    )
    for table_name in tables:
        table_description = tables[table_name]
        try:
            logging.info("Creating table {}".format(table_name))
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.info("Table {} already exists.".format(table_name))
            else:
                logging.exception(err.msg)
        else:
            logging.info("Table {} created.".format(table_name))
    close_connection_or_cursor(cursor)
    close_connection_or_cursor(connection)