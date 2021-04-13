import logging
import mysql.connector
from mysql.connector import errorcode

from ._helpers import get_connection, close_connection_or_cursor
from ..enums import *
from ..data.metrics import calculate_housing_metrics


def calculate_metrics(config=None):
    try:
        location_connection = get_connection(config)
        location_cursor = location_connection.cursor(dictionary=True)
        date_zhvi_connection = get_connection(config)
        date_zhvi_cursor = date_zhvi_connection.cursor(dictionary=True)
        calculation_connection = get_connection(config)
        calculation_cursor = calculation_connection.cursor()
        # select locations
        get_locations_data = "SELECT {column_location_id} FROM {table_name}".format(
            table_name=table_locations,
            column_location_id=column_location_id
        )
        location_cursor.execute(get_locations_data)
        for location in location_cursor:
            try:
                location_id = location[column_location_id]
                # for each location get all date_zhvi records
                get_locations_data = ("SELECT {column_date}, {column_zhvi} FROM {table_name} "
                                      "WHERE {column_location_id}={location_id}").format(
                    table_name=table_date_zhvi,
                    column_location_id=column_location_id,
                    location_id=location_id,
                    column_date=column_date,
                    column_zhvi=column_zhvi
                )
                date_zhvi_cursor.execute(get_locations_data)
                summary_data = calculate_housing_metrics(date_zhvi_cursor.fetchall())
                values = ("({column_location_id},{column_zhvi_start},{column_zhvi_end},'{column_date_start}',"
                          "'{column_date_end}',{column_zhvi_min},{column_zhvi_max},{column_date_difference},"
                          "{column_zhvi_percent_change})").format(
                    column_location_id=location_id,
                    column_zhvi_start=summary_data[custom_column_zhvi_start],
                    column_zhvi_end=summary_data[custom_column_zhvi_end],
                    column_date_start=summary_data[custom_column_start_date],
                    column_date_end=summary_data[custom_column_end_date],
                    column_zhvi_min=summary_data[custom_column_zhvi_max],
                    column_zhvi_max=summary_data[custom_column_zhvi_min],
                    column_date_difference=summary_data[custom_column_years_difference],
                    column_zhvi_percent_change=summary_data[custom_column_appreciation]
                )
                insert_calculation_data = ("INSERT INTO {table_name} "
                                           "({column_location_id},{column_zhvi_start},{column_zhvi_end},"
                                           "{column_date_start},{column_date_end},{column_zhvi_min},{column_zhvi_max},"
                                           "{column_date_difference},{column_zhvi_percent_change}) "
                                           "VALUES {values}").format(
                    table_name=table_calculations,
                    values=values,
                    column_location_id=column_location_id,
                    column_zhvi_start=column_zhvi_start,
                    column_zhvi_end=column_zhvi_end,
                    column_date_start=column_date_start,
                    column_date_end=column_date_end,
                    column_zhvi_min=column_zhvi_min,
                    column_zhvi_max=column_zhvi_max,
                    column_date_difference=column_date_difference,
                    column_zhvi_percent_change=column_zhvi_percent_change
                )
                calculation_cursor.execute(insert_calculation_data)
                # Make sure data is committed to the mysql_database
                calculation_connection.commit()
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_DUP_ENTRY:
                    logging.info("Metric already exists.")
                else:
                    logging.exception(err.msg)
        close_connection_or_cursor(location_cursor)
        close_connection_or_cursor(location_connection)
        close_connection_or_cursor(date_zhvi_cursor)
        close_connection_or_cursor(date_zhvi_connection)
        close_connection_or_cursor(calculation_cursor)
        close_connection_or_cursor(calculation_connection)
    except mysql.connector.Error as err:
        logging.exception(err.msg)
    except Exception as err:
        logging.exception(err)
