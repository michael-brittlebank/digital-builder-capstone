import pandas as pd
import numpy as np

from ..files import *
from ._helpers import *


def get_baseline_data(is_only_amfam_data):
    """
    get_baseline_data translates csv files into analysable files
    :param is_only_amfam_data: boolean for whether to return data in amfam's operating states or all states
    :return: list of baseline data
    """
    # todo, replace with db call
    raw_condo_data = import_csv("ingested_condo.csv")
    raw_sfr_data = import_csv("ingested_singlefamilyresidence.csv")

    conda_data = get_dataframe_from_list(raw_condo_data)
    sfr_data = get_dataframe_from_list(raw_sfr_data)


    return "hellow orld "+str(is_only_amfam_data)