import logging
import mysql.connector
from mysql.connector import errorcode

from ..enums import *
from ._helpers import get_connection, close_connection_or_cursor


def select_location_densities():
    return 0


def select_location_density_by_zip(zipcode):
    connection = get_connection()
    cursor = connection.cursor(buffered=True, dictionary=True)
    location = None
    try:
        get_location_data = ("SELECT * "
                             "FROM {table_name} "
                             "WHERE {column_region_name}={region_name} "
                             "LIMIT 1").format(
            table_name=table_location_density,
            region_name=zipcode,
            column_region_name=column_region_name,
        )
        cursor.execute(get_location_data)
        if cursor.rowcount > 0:
            location = cursor.fetchone()
    except mysql.connector.Error as err:
        logging.exception(err.msg)
    except Exception as err:
        logging.exception(err)
    close_connection_or_cursor(cursor)
    close_connection_or_cursor(connection)
    return location


def insert_location_density(zipcode, amfam_agency_count):
    connection = get_connection()
    cursor = connection.cursor()
    region_name = None
    location_id = None
    try:
        values = "({region_name},{amfam_agency_count})".format(
            region_name=zipcode,
            amfam_agency_count=amfam_agency_count
        )
        insert_location_data = ("INSERT INTO {table_name} ({column_region_name}, {column_amfam_agency_count}) "
                                "VALUES {values}").format(
            table_name=table_location_density,
            values=values,
            column_region_name=column_region_name,
            column_amfam_agency_count=column_amfam_agency_count
        )
        cursor.execute(insert_location_data)
        # Make sure data is committed to the database
        connection.commit()
        location_id = cursor.lastrowid
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            logging.info("Location density {} already exists.".format(region_name))
        else:
            logging.exception(err.msg)
    except Exception as err:
        logging.exception(err)
    close_connection_or_cursor(cursor)
    close_connection_or_cursor(connection)
    return location_id
