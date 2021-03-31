import pandas as pd
import numpy as np

from ..files import *
from ._helpers import *
from ..enums import *


def get_baseline_data(is_only_amfam_data):
    """
    get_baseline_data translates csv files into analysable files
    :param is_only_amfam_data: boolean for whether to return data in amfam's operating states or all states
    :return: list of baseline data
    """
    default_groupby = [zillow_column_region_name, custom_column_housing_type]

    # todo, replace with db call
    raw_condo_data = import_csv("ingested_condo.csv")
    raw_sfr_data = import_csv("ingested_singlefamilyresidence.csv")

    conda_data = get_dataframe_from_list(raw_condo_data)
    sfr_data = get_dataframe_from_list(raw_sfr_data)

    dataframe = pd.concat([conda_data, sfr_data])

    # temp while reading from file
    dataframe[custom_column_zhvi] = dataframe[custom_column_zhvi].astype(float) # cast string to float

    dataframe[custom_column_date] = pd.to_datetime(dataframe[custom_column_date])  # convert to pandas date
    dataframe[custom_column_zhvi] = dataframe[custom_column_zhvi].replace([0], np.nan)  # replace zero ZHVI with NAN to exclude from calculations
    dataframe.sort_values(by=[custom_column_date])  # sorting the data allows for percent change to be calculated correctly

    return dataframe.tail().to_csv()