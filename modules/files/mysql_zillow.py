import json

import mysql.connector
from mysql.connector import connect, Error, errorcode, pooling
import logging
import os
import datetime

from modules.enums import *
from ..data.baseline import calculate_baseline_data

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
            populate_housing_types()
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
        "  `housing_type_id` int NOT NULL AUTO_INCREMENT,"
        "  `housing_type` varchar(255) NOT NULL UNIQUE,"
        "  PRIMARY KEY (`housing_type_id`)"
        ") ENGINE=InnoDB").format(
        table_name=table_housing_type
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
        "  `region_name` int NOT NULL,"
        "  `state` varchar(2) NOT NULL,"
        "  `city` varchar(255) NOT NULL,"
        "  `metro` varchar(255) NOT NULL,"
        "  `county` varchar(255) NOT NULL,"
        "  PRIMARY KEY (`{column_location_id}`),"
        "  UNIQUE KEY (`region_name`,`{housing_type_id}`), KEY `housing_type_id` (`housing_type_id`),"
        "  FOREIGN KEY (`{housing_type_id}`) "
        "  REFERENCES `{housing_type_table}` (`{housing_type_id}`) ON DELETE CASCADE"
        ") ENGINE=InnoDB").format(
        table_name=table_locations,
        housing_type_table=table_housing_type,
        column_location_id=column_location_id,
        housing_type_id="housing_type_id"
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


def populate_housing_types():
    connection = get_connection()
    cursor = connection.cursor()
    add_housing_type = ("INSERT INTO {table_name} "
                        "(housing_type) "
                        "VALUES (%s)").format(
        table_name=table_housing_type
    )
    for housing_type in zillow_data_housing_types:
        try:
            cursor.execute(add_housing_type, (housing_type,))
            # Make sure data is committed to the database
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


def get_location_by_region_name_and_housing_type(region_name, housing_type_id):
    connection = get_connection()
    cursor = connection.cursor(buffered=True)
    location = None
    try:
        get_location_data = ("SELECT * FROM {table_name} "
                             "WHERE region_name={region_name} "
                             "AND WHERE housing_type_id=housing_type_id "
                             "LIMIT 1").format(
            table_name=table_locations,
            region_name=region_name,
            housing_type_id=housing_type_id
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


def get_housing_type_by_name(housing_type_name):
    connection = get_connection()
    cursor = connection.cursor(buffered=True)
    housing_type = None
    try:
        get_location_data = ("SELECT * FROM {table_name} "
                             "WHERE housing_type='{housing_type}' "
                             "LIMIT 1").format(
            table_name=table_housing_type,
            housing_type=housing_type_name
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


def insert_location(data, header_row, housing_type_id):
    connection = get_connection()
    cursor = connection.cursor()
    region_name = None
    location_id = None
    try:
        region_name = data[header_row.index(zillow_column_region_name)]
        values = "({housing_type_id},{region_name},'{state}', '{city}', '{metro}', '{county}')".format(
            housing_type_id=housing_type_id,
            region_name=region_name,
            state=data[header_row.index(zillow_column_state)],
            city=data[header_row.index(zillow_column_city)],
            metro=data[header_row.index(zillow_column_metro)],
            county=data[header_row.index(zillow_column_county_name)]
        )
        insert_location_data = ("INSERT INTO {table_name} "
                                "(housing_type_id, region_name, state, city, metro, county) "
                                "VALUES {values}").format(
            table_name=table_locations,
            values=values
        )
        cursor.execute(insert_location_data)
        # Make sure data is committed to the database
        connection.commit()
        location_id = cursor.lastrowid
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            logging.info("Location {} already exists.".format(region_name))
        else:
            logging.exception(err.msg)
    except Exception as err:
        logging.exception(err)
    close_connection_or_cursor(cursor)
    close_connection_or_cursor(connection)
    return location_id


def insert_housing_data(rows, header_row, data_type):
    add_housing_data = ""
    try:
        # get housing type id
        housing_type = get_housing_type_by_name(data_type)
        housing_type_id = housing_type[0]

        location_data = rows[0]  # assume only one location will be sent in a batch
        region_name = location_data[header_row.index(zillow_column_region_name)]
        # get location id
        location = get_location_by_region_name_and_housing_type(region_name, housing_type_id)
        # insert location if doesn't exist
        if not location:
            location_id = insert_location(location_data, header_row, housing_type_id)
        else:
            location_id = location[0]
        connection = get_connection()
        cursor = connection.cursor()
        values = []
        for row in rows:
            raw_datetime = row[header_row.index(custom_column_date)]
            formatted_date = datetime.datetime.strptime(raw_datetime, '%Y-%m-%d %H:%M:%S')
            values.append(
                "({location_id},'{date}',{zhvi})".format(
                    location_id=location_id,
                    date=formatted_date,
                    zhvi=row[header_row.index(custom_column_zhvi)]
                )
            )
        add_housing_data = (
            ("INSERT INTO {table_name} "
             "({column_location_id}, {column_date}, {column_zhvi}) "
             "VALUES {values}").format(
                table_name=table_date_zhvi,
                column_location_id=column_location_id,
                column_zhvi=column_zhvi,
                column_date=column_date,
                values=",".join(values)
            )
        )
        cursor.execute(add_housing_data)
        # Make sure data is committed to the database
        connection.commit()
        close_connection_or_cursor(cursor)
        close_connection_or_cursor(connection)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            logging.info("Housing data already exists {}".format(add_housing_data))
        else:
            logging.exception(err.msg)
    except Exception as err:
        logging.exception(err)


def get_zillow_data(limit=10000, config=None):
    # currently not ready for production without passing in a limit
    data = []
    try:
        connection = get_connection(config)
        cursor = connection.cursor()
        get_location_data = (
            "SELECT {location_table}.region_name, {housing_type_table}.housing_type, {location_table}.state, "
            "{location_table}.city, {location_table}.metro, {location_table}.county, {date_zhvi_table}.{column_date}, "
            "{date_zhvi_table}.{column_zhvi} "
            "FROM {date_zhvi_table} "
            "INNER JOIN {location_table} ON {date_zhvi_table}.{column_location_id}={location_table}.{column_location_id} "
            "INNER JOIN {housing_type_table} ON {location_table}.housing_type_id={housing_type_table}.housing_type_id "
            "LIMIT {limit}"
        ).format(
            location_table=table_locations,
            housing_type_table=table_housing_type,
            date_zhvi_table=table_date_zhvi,
            column_location_id=column_location_id,
            column_zhvi=column_zhvi,
            limit=limit
        )
        cursor.execute(get_location_data)
        data = cursor.fetchall()
        close_connection_or_cursor(cursor)
        close_connection_or_cursor(connection)
    except mysql.connector.Error as err:
        logging.exception(err.msg)
    except Exception as err:
        logging.exception(err)
    return data


def get_locations():
    connection = get_connection()
    cursor = connection.cursor(buffered=True)
    location = None
    try:
        get_locations_data = "SELECT * FROM {table_name}".format(
            table_name=table_locations,
        )
        cursor.execute(get_locations_data)
        if cursor.rowcount > 0:
            location = cursor.fetchone()
    except mysql.connector.Error as err:
        logging.exception(err.msg)
    except Exception as err:
        logging.exception(err)
    close_connection_or_cursor(cursor)
    close_connection_or_cursor(connection)
    return location


def calculate_metrics(config=None):
    try:
        location_connection = get_connection(config)
        location_cursor = location_connection.cursor(dictionary=True)
        date_zhvi_connection = get_connection(config)
        date_zhvi_cursor = date_zhvi_connection.cursor(dictionary=True)
        calculation_connection = get_connection(config)
        calculation_cursor = calculation_connection.cursor()
        # select locations
        get_locations_data = "SELECT {column_location_id} FROM {table_name} WHERE location_id=4 LIMIT 1".format(
            table_name=table_locations,
            column_location_id=column_location_id
        )
        location_cursor.execute(get_locations_data)
        for location in location_cursor:
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
            summary_data = calculate_baseline_data(date_zhvi_cursor.fetchall())
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
        close_connection_or_cursor(location_cursor)
        close_connection_or_cursor(location_connection)
        close_connection_or_cursor(date_zhvi_cursor)
        close_connection_or_cursor(date_zhvi_connection)
        close_connection_or_cursor(calculation_cursor)
        close_connection_or_cursor(calculation_connection)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            logging.info("Metric already exists.")
        else:
            logging.exception(err.msg)
    except Exception as err:
        logging.exception(err)
