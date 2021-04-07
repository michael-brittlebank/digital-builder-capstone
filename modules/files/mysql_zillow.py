import mysql.connector
from mysql.connector import connect, Error, errorcode, pooling
import logging
import os
import datetime
import json

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
            logging.exception(e)
    try:
        connection = _connection_pool.get_connection()
        return connection
    except Error as e:
        logging.exception(e)
        return None


def close_connection_or_cursor(connection_cursor):
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
    tables[table_summary] = (
        "CREATE TABLE `{table_name}` ("
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
        ") ENGINE=InnoDB").format(
            table_name=table_summary,
            housing_type_table=table_housing_type,
            housing_type_id="housing_type_id"
        )
    tables[table_date_zhvi] = (
        "CREATE TABLE `{table_name}` ("
        "  `date_zhvi_id` int NOT NULL AUTO_INCREMENT,"
        "  `{location_id}` int NOT NULL,"
        "  `date` datetime NOT NULL,"
        "  `zhvi` int NOT NULL,"
        "  PRIMARY KEY (`date_zhvi_id`),"
        "  UNIQUE KEY (`date`,`{location_id}`), KEY `location_id` (`location_id`),"
        "  FOREIGN KEY (`{location_id}`) "
        "  REFERENCES `{table_locations}` (`{location_id}`) ON DELETE CASCADE"
        ") ENGINE=InnoDB").format(
            table_name=table_date_zhvi,
            table_locations=table_locations,
            location_id="location_id"
        )
    tables[table_locations] = (
        "CREATE TABLE `{table_name}` ("
        "  `location_id` int NOT NULL AUTO_INCREMENT,"
        "  `{housing_type_id}` int NOT NULL,"
        "  `region_name` int NOT NULL,"
        "  `state` varchar(2) NOT NULL,"
        "  `city` varchar(255) NOT NULL,"
        "  `metro` varchar(255) NOT NULL,"
        "  `county` varchar(255) NOT NULL,"
        "  PRIMARY KEY (`location_id`),"
        "  UNIQUE KEY (`region_name`,`{housing_type_id}`), KEY `housing_type_id` (`housing_type_id`),"
        "  FOREIGN KEY (`{housing_type_id}`) "
        "  REFERENCES `{housing_type_table}` (`{housing_type_id}`) ON DELETE CASCADE"
        ") ENGINE=InnoDB").format(
            table_name=table_locations,
            housing_type_table=table_housing_type,
            housing_type_id="housing_type_id"
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


def get_location_by_region_name(region_name):
    connection = get_connection()
    cursor = connection.cursor(buffered=True)
    location = None
    try:
        get_location_data = ("SELECT * FROM {table_name} "
                             "WHERE region_name={region_name} "
                             "LIMIT 1").format(
            table_name=table_locations,
            region_name=region_name
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


def insert_location(data, header_row):
    connection = get_connection()
    cursor = connection.cursor()
    region_name = None
    location_id = None
    try:
        region_name = data[header_row.index(zillow_column_region_name)]
        values = "({housing_type_id},{region_name},'{state}', '{city}', '{metro}', '{county}')".format(
            housing_type_id=1,  # todo, replace with query
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


def insert_housing_data(rows, header_row):
    connection = get_connection()
    cursor = connection.cursor()
    try:
        location_data = rows[0]  # assume only one location will be sent in a batch
        # get location id
        location = get_location_by_region_name(location_data[header_row.index(zillow_column_region_name)])
        # insert location if doesn't exist
        if not location:
            location_id = insert_location(location_data, header_row)
        else:
            location_id = location[0]
        values = []
        for row in rows:
            raw_datetime = row[header_row.index(custom_column_date)]
            formatted_date = datetime.datetime.strptime(raw_datetime, '%Y-%m-%d %H:%M:%S')
            values.append(
                "({location_id},'{date}',{zhvi})".format(
                    location_id=int(location_id),
                    date=formatted_date,
                    zhvi=int(row[header_row.index(custom_column_zhvi)])
                )
            )
        add_housing_data = (
            ("INSERT INTO {table_name} "
             "(location_id, date, zhvi) "
             "VALUES {values}").format(
                table_name=table_date_zhvi,
                values=",".join(values)
            )
        )
        cursor.execute(add_housing_data)
        # Make sure data is committed to the database
        connection.commit()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            logging.info("Housing data already exists")
        else:
            logging.exception(err.msg)
    except Exception as err:
        logging.exception(err)
    close_connection_or_cursor(cursor)
    close_connection_or_cursor(connection)
