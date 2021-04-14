import logging
import mysql.connector
from mysql.connector import errorcode

from ._helpers import get_connection, close_connection_or_cursor
from ..enums import *
from ..data.metrics import calculate_housing_metrics
from .housing_types import select_housing_type_by_name


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
                # Make sure data is committed to the database
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


def calculate_average_zhvi(config=None):
    try:
        select_connection = get_connection(config)
        select_cursor = select_connection.cursor(dictionary=True)
        insert_connection = get_connection(config)
        insert_cursor = insert_connection.cursor(dictionary=True)
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
                        " GROUP BY YEAR({column_date})").format(
                        date_zhvi_table=table_date_zhvi,
                        locations_table=table_locations,
                        housing_type_table=table_housing_type,
                        column_location_id=column_location_id,
                        column_date=column_date,
                        year=year,
                        housing_type_id=housing_type_id,
                        column_zhvi=column_zhvi,
                        column_housing_type_id=column_housing_type_id,
                        column_year=column_year,
                        column_average_zhvi=column_average_zhvi
                    )
                    select_cursor.execute(get_average_zhvi_data)
                    average_zhvi_data = select_cursor.fetchall()
                    average_zhvi_data = average_zhvi_data[0]  # assume only one row
                    values = "({column_housing_type_id},{column_year},{column_average_zhvi})".format(
                        column_housing_type_id=housing_type_id,
                        column_year=average_zhvi_data[column_year],
                        column_average_zhvi=average_zhvi_data[column_average_zhvi],
                    )
                    insert_average_zhvi_data = ("INSERT INTO {table_name} "
                                                "({column_housing_type_id},{column_year},{column_average_zhvi}) "
                                                "VALUES {values}").format(
                        table_name=table_average_zhvi,
                        values=values,
                        column_housing_type_id=column_housing_type_id,
                        column_year=column_year,
                        column_average_zhvi=column_average_zhvi
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
