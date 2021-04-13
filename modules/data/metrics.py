import pandas as pd
import numpy as np

from ._helpers import get_home_appreciation_percentage_per_year
from ..enums import *


def calculate_housing_metrics(data):
    header_mappings = {column_date: custom_column_date, column_zhvi: custom_column_zhvi}

    # create dataframe
    base_dataframe = pd.DataFrame(data).rename(columns=header_mappings)

    # sorting the data allows for percent change to be calculated correctly
    base_dataframe = base_dataframe.sort_values(custom_column_date, ascending=True)

    # replace zero ZHVI with NAN to exclude from calculations
    base_dataframe[custom_column_zhvi] = base_dataframe[custom_column_zhvi].replace([0], np.nan)

    # convert to pandas date
    base_dataframe[custom_column_date] = pd.to_datetime(base_dataframe[custom_column_date])

    first_valid_zhvi_index = base_dataframe[custom_column_zhvi].first_valid_index()

    # create summary dataframe
    summary_dataframe = {
        custom_column_zhvi_start: base_dataframe[custom_column_zhvi].iat[first_valid_zhvi_index],
        custom_column_start_date: base_dataframe[custom_column_date].iat[first_valid_zhvi_index],
        custom_column_zhvi_end: base_dataframe[custom_column_zhvi].iat[-1],
        custom_column_end_date: base_dataframe[custom_column_date].max(),
        custom_column_zhvi_min: base_dataframe[custom_column_zhvi].min(),
        custom_column_zhvi_max: base_dataframe[custom_column_zhvi].max()
    }

    summary_dataframe[custom_column_years_difference] = round(
        (summary_dataframe[custom_column_end_date] - summary_dataframe[custom_column_start_date]).days / 365,
        1)  # estimate years from number of days between start and end
    summary_dataframe[custom_column_appreciation] = get_home_appreciation_percentage_per_year(
        summary_dataframe[custom_column_zhvi_start],
        summary_dataframe[custom_column_zhvi_end],
        summary_dataframe[custom_column_years_difference])
    return summary_dataframe
