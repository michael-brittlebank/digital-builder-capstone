import pandas as pd
import numpy as np

from ..files import *
from ._helpers import *
from ..enums import *


def get_baseline_data(is_only_amfam_data, is_raw_data):
    """
    get_baseline_data translates csv files into analysable files
    :param is_only_amfam_data: boolean for whether to return data in amfam's operating states or all states
    :param is_raw_data: boolean for whether to return formatted data or not
    :return: list of baseline data
    """
    default_groupby = [zillow_column_region_name, custom_column_housing_type]
    custom_column_appreciation = '% Per Year'
    custom_column_zhvi_start = 'ZHVI Start'
    custom_column_zhvi_end = 'ZHVI End'
    custom_column_years_difference = 'Years Diff'
    custom_column_start_date = 'Start Date'
    custom_column_end_date = 'End Date'

    # todo, replace with db call
    raw_condo_data = import_csv("ingested_condo.csv")
    raw_sfr_data = import_csv("ingested_singlefamilyresidence.csv")

    # convert data to dataframes and merge together
    conda_data = get_dataframe_from_list(raw_condo_data)
    sfr_data = get_dataframe_from_list(raw_sfr_data)
    base_dataframe = pd.concat([conda_data, sfr_data])

    # temp while reading from file
    base_dataframe[custom_column_zhvi] = base_dataframe[custom_column_zhvi].astype(float)  # cast string to float

    # massage data in dataframes
    base_dataframe[custom_column_date] = pd.to_datetime(base_dataframe[custom_column_date])  # convert to pandas date
    base_dataframe[custom_column_zhvi] = base_dataframe[custom_column_zhvi].replace([0],
                                                                                    np.nan)  # replace zero ZHVI with NAN to exclude from calculations
    base_dataframe.sort_values(
        by=[custom_column_date])  # sorting the data allows for percent change to be calculated correctly

    # calculate first valid zhvi value per row
    first_valid_dataframe = base_dataframe.pivot(index=default_groupby, columns=custom_column_date,
                                                 values=custom_column_zhvi)
    first_valid_zhvi = first_valid_dataframe.apply(pd.Series.first_valid_index, axis=1)

    # create summary dataframe
    summary_dataframe = pd.DataFrame()
    summary_dataframe[zillow_column_city] = base_dataframe.groupby(default_groupby)[zillow_column_city].first()
    summary_dataframe[zillow_column_state] = base_dataframe.groupby(default_groupby)[zillow_column_state].first()

    if is_only_amfam_data:
        summary_dataframe = filter_dataframe_by_amfam_states(summary_dataframe)

    # columns
    zhvi_start = base_dataframe.groupby(default_groupby)[
        custom_column_zhvi].first()
    zhvi_end = base_dataframe.groupby(default_groupby)[
        custom_column_zhvi].last()
    zhvi_min = base_dataframe.groupby(default_groupby)[custom_column_zhvi].min()
    zhvi_max = base_dataframe.groupby(default_groupby)[custom_column_zhvi].max()

    # format currencies
    if not is_raw_data:
        zhvi_start = zhvi_start.apply(currency_formatter)
        zhvi_end = zhvi_end.apply(currency_formatter)
        zhvi_min = zhvi_min.apply(currency_formatter)
        zhvi_max = zhvi_max.apply(currency_formatter)

    summary_dataframe[custom_column_zhvi_start] = zhvi_start
    summary_dataframe[custom_column_start_date] = first_valid_zhvi
    summary_dataframe[custom_column_zhvi_end] = zhvi_end
    summary_dataframe[custom_column_end_date] = base_dataframe.groupby(default_groupby)[custom_column_date].last()
    summary_dataframe['ZHVI Min'] = zhvi_min
    summary_dataframe['ZHVI Max'] = zhvi_max
    summary_dataframe[custom_column_years_difference] = round(
        (summary_dataframe[custom_column_end_date] - summary_dataframe[custom_column_start_date]).dt.days / 365,
        1)  # estimate years from number of days between start and end
    summary_dataframe[custom_column_appreciation] = summary_dataframe.apply(
        lambda row: get_home_appreciation_percentage_per_year(row[custom_column_zhvi_start],
                                                              row[custom_column_zhvi_end],
                                                              row[custom_column_years_difference],
                                                              is_raw_data), axis=1)
    summary_dataframe = summary_dataframe.sort_values(custom_column_appreciation,
                                                      ascending=False)  # sort table to find highest movers
    # format percentages
    if not is_raw_data:
        summary_dataframe[custom_column_appreciation] = summary_dataframe[custom_column_appreciation].apply(
            percent_formatter)
    return summary_dataframe
