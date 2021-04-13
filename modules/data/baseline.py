from ._helpers import *
from ..enums import *


def get_baseline_data(is_only_amfam_data, housing_type, is_raw_data):
    """
    get_baseline_data translates csv files into analysable files
    :param is_only_amfam_data: boolean for whether to return data in amfam's operating states or all states
    :param is_raw_data: boolean for whether to return formatted data or not
    :return: list of baseline data
    """

    # create dataframe
    base_dataframe = pd.DataFrame(raw_baseline_data).rename(columns=header_mappings)

    # massage data in dataframes
    base_dataframe[custom_column_zhvi] = base_dataframe[custom_column_zhvi].astype(float)  # cast string to float
    base_dataframe[custom_column_date] = pd.to_datetime(base_dataframe[custom_column_date])  # convert to pandas date

    summary_dataframe[zillow_column_city] = base_dataframe.groupby(default_groupby)[zillow_column_city].first()
    summary_dataframe[zillow_column_state] = base_dataframe.groupby(default_groupby)[zillow_column_state].first()

    summary_dataframe = summary_dataframe.sort_values(custom_column_appreciation,
                                                      ascending=False)  # sort table to find highest movers

    return summary_dataframe
