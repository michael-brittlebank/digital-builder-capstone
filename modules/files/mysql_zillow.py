import mysql.connector
from mysql.connector import connect, Error, errorcode, pooling
import logging
import os

from modules.enums import *

_connection_pool = None


def get_connection():
    global _connection_pool
    if not _connection_pool:
        try:
            _connection_pool = pooling.MySQLConnectionPool(
                host=os.getenv(env_database_host),
                user=os.getenv(env_database_username),
                password=os.getenv(env_database_password),
                database="capstone",
                pool_name="capstone_pool")
            # check to ensure tables exist
            create_application_tables()
            # populate tables as needed
            populate_housing_types()
        except Error as e:
            logging.error(e)
    try:
        connection = _connection_pool.get_connection()
        return connection
    except Error as e:
        logging.error(e)
        return None


def close_connection_or_cursor(connection_cursor):
    connection_cursor.close()


def create_application_tables():
    connection = get_connection()
    cursor = connection.cursor()
    tables = {}
    tables[table_housing_type] = (
        "CREATE TABLE `{}` ("
        "  `housing_type_id` int NOT NULL AUTO_INCREMENT,"
        "  `housing_type` varchar(255) NOT NULL UNIQUE,"
        "  PRIMARY KEY (`housing_type_id`)"
        ") ENGINE=InnoDB".format(table_housing_type))
    tables[table_summary] = (
        "CREATE TABLE `{summary_table}` ("
        "  `summary_id` varchar(255) NOT NULL,"
        "  `{housing_type_id}` int NOT NULL,"
        "  `percent_min` double NOT NULL,"
        "  `percent_max` double NOT NULL,"
        "  `percent_average` double NOT NULL,"
        "  `years_min` double NOT NULL,"
        "  `years_max` double NOT NULL,"
        "  `years_average` double NOT NULL,"
        "  PRIMARY KEY (`summary_id`),"
        "  FOREIGN KEY (`{housing_type_id}`) "
        "  REFERENCES `{housing_type_table}` (`{housing_type_id}`) ON DELETE CASCADE"
        ") ENGINE=InnoDB".format(
            summary_table=table_summary,
            housing_type_table=table_housing_type,
            housing_type_id="housing_type_id"
        ))
    tables[table_housing_by_zip] = (
        "CREATE TABLE `{housing_table}` ("
        "  `{housing_type_id}` int NOT NULL,"
        "  `region_name` int NOT NULL,"
        "  `state` varchar(2) NOT NULL,"
        "  `city` varchar(255) NOT NULL,"
        "  `metro` varchar(255) NOT NULL,"
        "  `county` varchar(255) NOT NULL,"
        "  `date` datetime NOT NULL,"
        "  `zhvi` int NOT NULL,"
        "  `region_id` int NOT NULL,"
        "  PRIMARY KEY (`region_name`,`{housing_type_id}`), KEY `housing_type_id` (`housing_type_id`),"
        "  FOREIGN KEY (`{housing_type_id}`) "
        "  REFERENCES `{housing_type_table}` (`{housing_type_id}`) ON DELETE CASCADE"
        ") ENGINE=InnoDB".format(
            housing_table=table_housing_by_zip,
            housing_type_table=table_housing_type,
            housing_type_id="housing_type_id"
        ))
    for table_name in tables:
        table_description = tables[table_name]
        try:
            logging.warning("Creating table {}".format(table_name))
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.warning("Table {} already exists.".format(table_name))
            else:
                logging.warning(err.msg)
        else:
            logging.warning("Table {} created.".format(table_name))
    close_connection_or_cursor(cursor)
    close_connection_or_cursor(connection)


def populate_housing_types():
    connection = get_connection()
    cursor = connection.cursor()
    logging.warning("populate")
    try:
        add_housing_type = (
            "INSERT INTO {} "
            "(housing_type) "
            "VALUES (%s)".format(
                table_housing_type
            )
        )
        for housing_type in zillow_data_housing_types:
            cursor.execute(add_housing_type, (housing_type,))
        # Make sure data is committed to the database
        connection.commit()
    except mysql.connector.Error as err:
        logging.warning(err.msg)
    except Exception as err:
        logging.warning(err)
    close_connection_or_cursor(cursor)
    close_connection_or_cursor(connection)
