import logging
import mysql.connector
from mysql.connector import errorcode

from ._helpers import get_connection, close_connection_or_cursor, get_amfam_only_condition
from ..enums import *
from .location_housing_types import select_housing_type_by_name


def calculate_average_zhvi(is_only_amfam_data, config=None):
    try:
        select_connection = get_connection(config)
        select_cursor = select_connection.cursor(dictionary=True)
        insert_connection = get_connection(config)
        insert_cursor = insert_connection.cursor(dictionary=True)
        amfam_operating_states_condition = "" if not is_only_amfam_data else get_amfam_only_condition()
        for housing_type_name in zillow_data_housing_types:
            # get housing type id
            housing_type = select_housing_type_by_name(housing_type_name)
            housing_type_id = housing_type[column_housing_type_id]

            # db call
            for year in range(zillow_data_start, zillow_data_end + 1):  # inclusive range
                try:
                    # for each year calculate average zhvi by housing type id
                    get_average_zhvi_data = (
                        "SELECT YEAR({column_date}) as {column_year}, AVG({column_zhvi}) as {column_average_zhvi} "
                        " FROM {date_zhvi_table} "
                        " INNER JOIN {locations_table} ON {date_zhvi_table}.{column_location_id}={locations_table}.{"
                        "column_location_id} "
                        " INNER JOIN {housing_type_table} ON {locations_table}.{column_housing_type_id}={"
                        "housing_type_table}.{column_housing_type_id} "
                        " WHERE {housing_type_table}.{column_housing_type_id}={housing_type_id} "
                        " AND YEAR({column_date})={year} "
                        " {amfam_operating_states_condition} "
                        " GROUP BY YEAR({column_date})").format(
                        date_zhvi_table=table_zhvi_month,
                        locations_table=table_locations,
                        housing_type_table=table_location_housing_type,
                        column_location_id=column_location_id,
                        column_date=column_date,
                        year=year,
                        housing_type_id=housing_type_id,
                        column_zhvi=column_zhvi,
                        column_housing_type_id=column_housing_type_id,
                        column_year=column_year,
                        column_average_zhvi=column_average_zhvi,
                        amfam_operating_states_condition=amfam_operating_states_condition
                    )
                    select_cursor.execute(get_average_zhvi_data)
                    average_zhvi_data = select_cursor.fetchall()
                    average_zhvi_data = average_zhvi_data[0]  # assume only one row
                    values = "({column_housing_type_id},{column_year},{column_average_zhvi}, {column_amfam_only})".format(
                        column_housing_type_id=housing_type_id,
                        column_year=average_zhvi_data[column_year],
                        column_average_zhvi=average_zhvi_data[column_average_zhvi],
                        column_amfam_only=amfam_only_states_indicator if is_only_amfam_data else all_states_indicator
                    )
                    insert_average_zhvi_data = ("INSERT INTO {table_name} "
                                                "({column_housing_type_id},{column_year},{column_average_zhvi},"
                                                "{column_amfam_only}) "
                                                "VALUES {values}").format(
                        table_name=table_zhvi_year,
                        values=values,
                        column_housing_type_id=column_housing_type_id,
                        column_year=column_year,
                        column_average_zhvi=column_average_zhvi,
                        column_amfam_only=column_amfam_only
                    )
                    insert_cursor.execute(insert_average_zhvi_data)
                    # Make sure data is committed to the database
                    insert_connection.commit()
                except mysql.connector.Error as err:
                    if err.errno == errorcode.ER_DUP_ENTRY:
                        logging.info("Average ZHVI already exists.")
                    else:
                        logging.exception(err.msg)
        close_connection_or_cursor(insert_cursor)
        close_connection_or_cursor(insert_connection)
        close_connection_or_cursor(select_cursor)
        close_connection_or_cursor(select_connection)
    except mysql.connector.Error as err:
        logging.exception(err.msg)
    except Exception as err:
        logging.exception(err)


def select_average_zhvi(config=None):
    data = []
    try:
        select_connection = get_connection(config)
        select_cursor = select_connection.cursor(dictionary=True)
        get_average_zhvi_data = (
            "SELECT {housing_type_table}.{column_housing_type}, {average_zhvi_table}.{column_year}, "
            "{average_zhvi_table}.{column_average_zhvi}, {average_zhvi_table}.{column_amfam_only} "
            " FROM {average_zhvi_table} "
            "INNER JOIN {housing_type_table} ON {housing_type_table}.{column_housing_type_id}={average_zhvi_table}.{"
            "column_housing_type_id} ").format(
            average_zhvi_table=table_zhvi_year,
            housing_type_table=table_location_housing_type,
            column_average_zhvi=column_average_zhvi,
            column_housing_type_id=column_housing_type_id,
            column_housing_type=column_housing_type,
            column_year=column_year,
            column_amfam_only=column_amfam_only
        )
        select_cursor.execute(get_average_zhvi_data)
        data = select_cursor.fetchall()
        close_connection_or_cursor(select_cursor)
        close_connection_or_cursor(select_connection)
    except mysql.connector.Error as err:
        logging.exception(err.msg)
    except Exception as err:
        logging.exception(err)
    return data
