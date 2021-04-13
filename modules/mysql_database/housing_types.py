import logging
import mysql.connector
from mysql.connector import errorcode

from ._helpers import get_connection, close_connection_or_cursor
from ..enums import *


def insert_housing_types():
    connection = get_connection()
    cursor = connection.cursor()
    add_housing_type = ("INSERT INTO {table_name} "
                        "({column_housing_type}) "
                        "VALUES (%s)").format(
        table_name=table_housing_type,
        column_housing_type=column_housing_type
    )
    for housing_type in zillow_data_housing_types:
        try:
            cursor.execute(add_housing_type, (housing_type,))
            # Make sure data is committed to the mysql_database
            connection.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_DUP_ENTRY:
                logging.info("Entry {} already exists.".format(housing_type))
            else:
                logging.exception(err.msg)
        except Exception as err:
            logging.exception(err)
    close_connection_or_cursor(cursor)
    close_connection_or_cursor(connection)


def select_housing_type_by_name(housing_type_name):
    connection = get_connection()
    cursor = connection.cursor(buffered=True, dictionary=True)
    housing_type = None
    try:
        get_location_data = ("SELECT * FROM {table_name} "
                             "WHERE {column_housing_type}='{housing_type}' "
                             "LIMIT 1").format(
            table_name=table_housing_type,
            housing_type=housing_type_name,
            column_housing_type=column_housing_type
        )
        cursor.execute(get_location_data)
        if cursor.rowcount > 0:
            housing_type = cursor.fetchone()
    except mysql.connector.Error as err:
        logging.exception(err.msg)
    except Exception as err:
        logging.exception(err)
    close_connection_or_cursor(cursor)
    close_connection_or_cursor(connection)
    return housing_type
