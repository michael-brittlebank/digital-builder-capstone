import datetime
import logging
import mysql.connector
from mysql.connector import errorcode

from ..enums import *
from .housing_types import select_housing_type_by_name
from .locations import select_location_by_region_name_and_housing_type, insert_location
from ._helpers import get_connection, close_connection_or_cursor


def insert_housing_data(rows, header_row, data_type):
    add_housing_data = ""
    try:
        # get housing type id
        housing_type = select_housing_type_by_name(data_type)
        housing_type_id = housing_type[column_housing_type_id]

        location_data = rows[0]  # assume only one location will be sent in a batch
        region_name = location_data[header_row.index(zillow_column_region_name)]
        # get location id
        location = select_location_by_region_name_and_housing_type(region_name, housing_type_id)
        # insert location if doesn't exist
        if not location:
            location_id = insert_location(location_data, header_row, housing_type_id)
        else:
            location_id = location[column_location_id]
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
    tables[table_average_zhvi] = (
        "CREATE TABLE `{table_name}` ("
        "  `{column_average_zhvi_id}` int NOT NULL AUTO_INCREMENT,"
        "  `{housing_type_id}` int NOT NULL,"
        "  `{column_year}` int NOT NULL,"
        "  `{column_average_zhvi}` int NOT NULL,"
        "  `{column_amfam_only}` TINYINT NOT NULL,"
        "  PRIMARY KEY (`{column_average_zhvi_id}`),"
        "  UNIQUE KEY (`{housing_type_id}`,`{column_year}`,`{column_amfam_only}`), "
        "  KEY `housing_type_id` (`housing_type_id`),"
        "  FOREIGN KEY (`{housing_type_id}`) "
        "  REFERENCES `{housing_type_table}` (`{housing_type_id}`) ON DELETE CASCADE"
        ") ENGINE=InnoDB").format(
        table_name=table_average_zhvi,
        column_average_zhvi_id="average_zhvi_id",
        housing_type_table=table_housing_type,
        housing_type_id=column_housing_type_id,
        column_year=column_year,
        column_average_zhvi=column_average_zhvi,
        column_amfam_only=column_amfam_only
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