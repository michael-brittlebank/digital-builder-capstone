import datetime

from .mysql import *
from .housing_types import select_housing_type_by_name
from .locations import select_location_by_region_name_and_housing_type, insert_location


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
