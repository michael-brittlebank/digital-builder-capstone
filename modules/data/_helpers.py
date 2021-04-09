import pandas as pd
import locale
import numpy as np
import math

from ..enums import *

locale.setlocale(locale.LC_ALL, '')


def get_dataframe_from_list(data, headers=None):
    """
    get_dataframe_from_list translates raw data to pandas dataframe
    :param data: raw data to convert
    :param headers: list of strings for column titles
    :return: dataframe
    """
    if headers is None:
        headers = data.pop(0)
    return pd.DataFrame(data, columns=headers)


def percent_formatter(value):
    """
    percent_formatter formats float to percent string
    :param value: float to format
    :return: string
    """
    if value == np.NaN or value is None:
        value = 0
    return "{:.2f}%".format(value)


def currency_formatter(value):
    """
    currency_formatter formats float to currency string
    :param value: float to format
    :return: string
    """
    if math.isnan(value):
        value = 0
    return "${}".format(locale.format_string('%d', value, True))


def currency_unformatter(value):
    """
    currency_formatter formats currency string to float
    :param value: string to format
    :return: float
    """
    return locale.atof(value.strip("$"))


def get_home_appreciation_percentage_per_year(initial_value, final_value, years):
    """
    currency_formatter formats currency string to float
    :param initial_value: initial home value
    :param final_value: final home value
    :param years: number of years to calculate appreciation over
    :return: float
    """
    # https://goodcalculators.com/home-appreciation-calculator/
    years = float(years)
    home_appreciation_percentage = (((final_value / initial_value) ** (1 / years)) - 1) * 100
    if math.isnan(home_appreciation_percentage):
        home_appreciation_percentage = 0
    return home_appreciation_percentage


def filter_dataframe_by_amfam_states(dataframe):
    """
    filter_dataframe_by_amfam_states filters a dataframe to only include amfam operating states
    :param dataframe: dataframe to modify
    :return: dataframe
    """
    return dataframe[dataframe.State.isin(amfam_territory_states)]


def get_zhvi_weighted_average():
    """
    get_zhvi_weighted_average is not production ready
    """
    data = pd.DataFrame()  # get_baseline_data(True, True)
    data = data.reset_index("Housing Type")  # remove multiindex to filter on housing type
    max_years = data["Years Diff"].max()
    data["ZHVI Per Year"] = (data['ZHVI End'] - data['ZHVI Start']) / data['Years Diff']
    weighted_average = (data['% Per Year'] * data["ZHVI Per Year"]).sum() / data['% Per Year'].sum()
    # https://stackoverflow.com/questions/2413522/weighted-standard-deviation-in-numpy
    variance = math.sqrt(np.average((data["ZHVI Per Year"] - weighted_average) ** 2, weights=data['% Per Year']))
    # https://math.stackexchange.com/questions/1912109/comparing-a-value-with-mean-and-standard-deviation
    data["Weighted Std Var"] = (data["ZHVI Per Year"] - weighted_average) / variance
    data = data.sort_values("Weighted Std Var", ascending=False)  # sort by weighted variance
    data.head(10)
    return False
