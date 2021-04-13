from .mysql import *


def select_location_by_region_name_and_housing_type(region_name, housing_type_id):
    connection = get_connection()
    cursor = connection.cursor(buffered=True)
    location = None
    try:
        get_location_data = ("SELECT * FROM {table_name} "
                             "WHERE {column_region_name}={region_name} "
                             "AND WHERE {column_housing_type_id}={housing_type_id} "
                             "LIMIT 1").format(
            table_name=table_locations,
            region_name=region_name,
            housing_type_id=housing_type_id,
            column_region_name=column_region_name,
            column_housing_type_id=column_housing_type_id
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
        insert_location_data = ("INSERT INTO {table_name} ({column_housing_type_id}, {column_region_name}, "
                                "{column_state}, {column_city}, metro, county) "
                                "VALUES {values}").format(
            table_name=table_locations,
            values=values,
            column_region_name=column_region_name,
            column_housing_type_id=column_housing_type_id,
            column_state=column_state,
            column_city=column_city
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


def select_locations():
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
