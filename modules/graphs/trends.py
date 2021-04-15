import pandas as pd
import numpy as np
import matplotlib
# https://stackoverflow.com/questions/15713279/calling-pylab-savefig-without-display-in-ipython/15713545#15713545
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from ._helpers import formatter_percent, formatter_currency
from ..files import export_graph
from ..enums import *
from ..database import select_average_zhvi


def get_zhvi_trend_graph(config=None):
    """
    get_baseline_graphs translates basedata into two exported graphs
    :return: pandas dataframe with summary table
    """
    header_mappings = {column_housing_type: custom_column_housing_type, column_average_zhvi: custom_column_zhvi_avg, column_year: custom_column_year}
    trend_data = select_average_zhvi(config)
    df = pd.DataFrame(trend_data).rename(columns=header_mappings)
    graph_title = "Average ZHVI Over Time"
    condo_data = df[df[custom_column_housing_type] == zillow_data_type_condo]
    sfr_data = df[df[custom_column_housing_type] == zillow_data_type_sfr]
    condo_amfam_data = condo_data[condo_data[column_amfam_only] == amfam_only_states_indicator]
    condo_all_data = condo_data[condo_data[column_amfam_only] == all_states_indicator]
    sfr_amfam_data = sfr_data[sfr_data[column_amfam_only] == amfam_only_states_indicator]
    sfr_all_data = sfr_data[sfr_data[column_amfam_only] == all_states_indicator]
    fig1 = plt.figure()  # Creating new figure
    ax1 = fig1.add_subplot()  # Creating axis
    ax1.set_title(graph_title)

    ax1.plot(sfr_all_data[custom_column_year], sfr_all_data[custom_column_zhvi_avg], linestyle="solid",
             label="SFR (All States)")
    ax1.plot(sfr_all_data[custom_column_year], condo_all_data[custom_column_zhvi_avg], linestyle="dotted",
             label="Condo (All States)")
    ax1.plot(sfr_all_data[custom_column_year], sfr_amfam_data[custom_column_zhvi_avg], linestyle="dashed",
             label="SFR (AmFam States)")
    ax1.plot(sfr_all_data[custom_column_year], condo_amfam_data[custom_column_zhvi_avg], linestyle="dashdot",
             label="Condo (AmFam States)")
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(formatter_currency))
    ax1.legend()

    export_graph(ax1, "zhvi-trend")

    return df


def build_summary_graph(dataframe, title):
    """
    build_summary_graph builds a summary graph for a dataframe looking at end ZHVI compared to overall percent increase
    :param dataframe: pandas dataframe
    :param title: graph title
    :return: pandas dataframe with summary table
    """
    custom_column_trend = "Trend"

    plot = dataframe.plot(kind='scatter', x=custom_column_zhvi_end, y=custom_column_appreciation, title=title)

    plot.xaxis.set_major_formatter(plt.FuncFormatter(formatter_currency))
    plot.yaxis.set_major_formatter(plt.FuncFormatter(formatter_percent))

    # https://towardsdatascience.com/regression-plots-with-pandas-and-numpy-faf2edbfad4f
    d = np.polyfit(dataframe[custom_column_zhvi_end], dataframe[custom_column_appreciation], 1)
    f = np.poly1d(d)

    dataframe.insert(len(dataframe.columns), custom_column_trend,
                     f(dataframe[custom_column_zhvi_end]))  # insert trend data at end

    dataframe = dataframe.plot(x=custom_column_zhvi_end, y=custom_column_trend, color='Red', ax=plot)
    return dataframe
