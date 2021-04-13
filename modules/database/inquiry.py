from .mysql import *


def select_baseline_data(is_only_amfam_data, housing_type_id, config=None):
    data = []
    amfam_operating_states_condition = ""
    if is_only_amfam_data:
        amfam_operating_states_condition = "AND {location_table}.{column_state} IN('{amfam_territory_states}')".format(
            location_table=table_locations,
            column_state=column_state,
            amfam_territory_states="','".join(amfam_territory_states))
    try:
        connection = get_connection(config)
        cursor = connection.cursor(dictionary=True)
        get_location_data = (
            "SELECT {location_table}.{column_region_name}, {housing_type_table}.{column_housing_type}, "
            "{location_table}.{column_state}, {location_table}.{column_city}, {column_zhvi_start}, "
            "{column_zhvi_end}, {column_zhvi_min}, {column_zhvi_max}, {column_date_start}, {column_date_end}, "
            "{column_date_difference}, {column_zhvi_percent_change} "
            " FROM {calculations_table}"
            " INNER JOIN {location_table} ON {calculations_table}.{column_location_id}={location_table}.{"
            "column_location_id} "
            " INNER JOIN {housing_type_table} ON {location_table}.{column_housing_type_id}={housing_type_table}.{"
            "column_housing_type_id}"
            " WHERE {location_table}.{column_housing_type_id} = {housing_type_id}"
            " AND {column_date_difference} > {min_years_data}"
            " {amfam_operating_states_condition}"
            " ORDER BY {column_zhvi_percent_change} DESC"
            " LIMIT 25"
        ).format(
            calculations_table=table_calculations,
            location_table=table_locations,
            housing_type_table=table_housing_type,
            date_zhvi_table=table_date_zhvi,
            column_region_name=column_region_name,
            column_state=column_state,
            column_city=column_city,
            column_location_id=column_location_id,
            column_zhvi=column_zhvi,
            column_housing_type_id=column_housing_type_id,
            column_housing_type=column_housing_type,
            column_zhvi_percent_change=column_zhvi_percent_change,
            column_zhvi_start=column_zhvi_start,
            column_zhvi_end=column_zhvi_end,
            column_zhvi_min=column_zhvi_min,
            column_zhvi_max=column_zhvi_max,
            column_date_start=column_date_start,
            column_date_end=column_date_end,
            column_date_difference=column_date_difference,
            min_years_data=5,  # at least 5 years of records are required for proper comparison
            housing_type_id=housing_type_id,
            amfam_operating_states_condition=amfam_operating_states_condition
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
