import pandas as pd
import locale

from ..enums import *

locale.setlocale(locale.LC_ALL, '')


def get_dataframe_from_list(data):
    """
    get_dataframe_from_list translates raw data to pandas dataframe
    :param data: raw data to convert
    :return: dataframe
    """
    headers = data.pop(0)
    return pd.DataFrame(data, columns=headers)


def percent_formatter(value):
    """
    percent_formatter formats float to percent string
    :param value: float to format
    :return: string
    """
    return "{:.2f}%".format(value)


def currency_formatter(value):
    """
    currency_formatter formats float to currency string
    :param value: float to format
    :return: string
    """
    return "${}".format(locale.format_string('%d', value, True))


def currency_unformatter(value):
    """
    currency_formatter formats currency string to float
    :param value: string to format
    :return: float
    """
    return locale.atof(value.strip("$"))


def get_home_appreciation_percentage_per_year(initial_value, final_value, years, is_raw_data):
    """
    currency_formatter formats currency string to float
    :param initial_value: initial home value
    :param final_value: final home value
    :param years: number of years to calculate appreciation over
    :param is_raw_data: boolean if data is formatted or not
    :return: float
    """
    # https://goodcalculators.com/home-appreciation-calculator/
    if not is_raw_data:
        final_value = currency_unformatter(final_value)
        initial_value = currency_unformatter(initial_value)
    years = float(years)
    return (((final_value / initial_value) ** (1 / years)) - 1) * 100


def filter_dataframe_by_amfam_states(dataframe):
    """
    filter_dataframe_by_amfam_states filters a dataframe to only include amfam operating states
    :param dataframe: dataframe to modify
    :return: dataframe
    """
    return dataframe[dataframe.State.isin(amfam_territory_states)]
