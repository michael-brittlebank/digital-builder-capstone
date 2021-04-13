import pandas as pd
from ..enums import *
from ._helpers import percent_formatter, double_formatter


def get_baseline_data(is_only_amfam_data, housing_type_name, is_raw_data):
    from ..database import select_housing_type_by_name, select_baseline_data

    # get housing type id
    housing_type = select_housing_type_by_name(housing_type_name)
    housing_type_id = housing_type[column_housing_type_id]

    # db call
    raw_data = select_baseline_data(is_only_amfam_data, housing_type_id)

    # create dataframe
    baseline_dataframe = pd.DataFrame(raw_data)

    # format percentages
    if not is_raw_data:
        baseline_dataframe[custom_column_percent_min] = baseline_dataframe[custom_column_percent_min].apply(
            percent_formatter)
        baseline_dataframe[custom_column_percent_max] = baseline_dataframe[custom_column_percent_max].apply(
            percent_formatter)
        baseline_dataframe[custom_column_percent_avg] = baseline_dataframe[custom_column_percent_avg].apply(
            percent_formatter)
        baseline_dataframe[custom_column_percent_stddev] = baseline_dataframe[custom_column_percent_stddev].apply(
            percent_formatter)
        baseline_dataframe[custom_column_years_min] = baseline_dataframe[custom_column_years_min].apply(
            double_formatter)
        baseline_dataframe[custom_column_years_max] = baseline_dataframe[custom_column_years_max].apply(
            double_formatter)
        baseline_dataframe[custom_column_years_avg] = baseline_dataframe[custom_column_years_avg].apply(
            double_formatter)
        baseline_dataframe[custom_column_years_stddev] = baseline_dataframe[custom_column_years_stddev].apply(
            double_formatter)

    return baseline_dataframe
