from ..files import *
from ._helpers import *
from ..enums import *


def get_forecast_data(is_raw_data):
    """
    get_baseline_data translates csv files into analysable files
    :param is_only_amfam_data: boolean for whether to return data in amfam's operating states or all states
    :param is_raw_data: boolean for whether to return formatted data or not
    :return: list of baseline data
    """
    default_groupby = [zillow_column_region_name, custom_column_housing_type]

    # todo, replace with db call
    raw_condo_data = import_csv("ingested_condo.csv")
    raw_sfr_data = import_csv("ingested_singlefamilyresidence.csv")

    # convert data to dataframes and merge together
    conda_data = get_dataframe_from_list(raw_condo_data)
    sfr_data = get_dataframe_from_list(raw_sfr_data)
    base_dataframe = pd.concat([conda_data, sfr_data])

    # temp while reading from file
    base_dataframe[custom_column_zhvi] = base_dataframe[custom_column_zhvi].astype(float)  # cast string to float

    return "pie"
