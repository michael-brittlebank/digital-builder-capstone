import pandas as pd

from ._helpers import percent_formatter, currency_formatter
from ..mysql_database import *


def get_leaders_data(is_only_amfam_data, housing_type_name, is_raw_data):

    # get housing type id
    housing_type = select_housing_type_by_name(housing_type_name)
    housing_type_id = housing_type[column_housing_type_id]

    # db call
    raw_data = select_leader_data(is_only_amfam_data, housing_type_id)

    header_mappings = {
        column_region_name: zillow_column_region_name,
        column_housing_type: custom_column_housing_type,
        column_state: zillow_column_state,
        column_city: zillow_column_city,
        column_zhvi_start: custom_column_zhvi_start,
        column_zhvi_end: custom_column_zhvi_end,
        column_zhvi_min: custom_column_zhvi_min,
        column_zhvi_max: custom_column_zhvi_max,
        column_date_start: custom_column_start_date,
        column_date_end: custom_column_end_date,
        column_date_difference: custom_column_years_difference,
        column_zhvi_percent_change: custom_column_appreciation
    }

    # create dataframe
    leaders_dataframe = pd.DataFrame(raw_data).rename(columns=header_mappings)

    # format percentages
    if not is_raw_data:
        leaders_dataframe[custom_column_appreciation] = leaders_dataframe[custom_column_appreciation].apply(
            percent_formatter)
        leaders_dataframe[custom_column_zhvi_start] = leaders_dataframe[custom_column_zhvi_start].apply(
            currency_formatter)
        leaders_dataframe[custom_column_zhvi_end] = leaders_dataframe[custom_column_zhvi_end].apply(currency_formatter)
        leaders_dataframe[custom_column_zhvi_min] = leaders_dataframe[custom_column_zhvi_min].apply(currency_formatter)
        leaders_dataframe[custom_column_zhvi_max] = leaders_dataframe[custom_column_zhvi_max].apply(currency_formatter)

    return leaders_dataframe
