import pandas as pd
import numpy as np
import matplotlib
# https://stackoverflow.com/questions/15713279/calling-pylab-savefig-without-display-in-ipython/15713545#15713545
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from ..data import get_baseline_data
from ._helpers import formatter_percent, formatter_currency
from ..files import export_graph
from ..enums import *


def get_baseline_graphs(is_only_amfam_data):
    """
    get_baseline_graphs translates basedata into two exported graphs
    :param is_only_amfam_data: boolean for whether to return data in amfam's operating states or all states
    :return: pandas dataframe with summary table
    """
    is_raw_data = True
    baseline_data = get_baseline_data(is_only_amfam_data, is_raw_data)
    dataframe = baseline_data.reset_index(custom_column_housing_type)  # remove multiindex to filter on housing type
    default_groupby = [custom_column_housing_type]

    graph_title_suffix = "(All States)"
    if is_only_amfam_data:
        graph_title_suffix = "(AmFam States)"
    # build condo graph
    graph_title = "Condos {}".format(graph_title_suffix)
    condo_graph = build_summary_graph(dataframe[dataframe[custom_column_housing_type] == zillow_data_type_condo], graph_title)
    export_graph(condo_graph, zillow_data_type_condo.lower())

    # build sfr graph
    graph_title = "SFR {}".format(graph_title_suffix)
    sfr_graph = build_summary_graph(dataframe[dataframe[custom_column_housing_type] == zillow_data_type_sfr], graph_title)
    export_graph(sfr_graph, zillow_data_type_sfr.lower())

    # build summary table
    summary_dataframe = pd.DataFrame()
    summary_dataframe["% Per Year Min"] = dataframe.groupby(default_groupby)[custom_column_appreciation].min()
    summary_dataframe["% Per Year Max"] = dataframe.groupby(default_groupby)[custom_column_appreciation].max()
    summary_dataframe["% Per Year Avg"] = dataframe.groupby(default_groupby)[custom_column_appreciation].mean()
    summary_dataframe["Years Diff Min"] = dataframe.groupby(default_groupby)[custom_column_years_difference].min()
    summary_dataframe["Years Diff Max"] = dataframe.groupby(default_groupby)[custom_column_years_difference].max()
    summary_dataframe["Years Diff Avg"] = dataframe.groupby(default_groupby)[custom_column_years_difference].mean()

    return summary_dataframe


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
