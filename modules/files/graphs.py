from ._helpers import *
from datetime import datetime

from ..enums import *


def export_graph(pandas_plot, filename):
    """
    export_graph tries to export a plot figure to the local directory
    :param pandas_plot: pandas plot object
    :param filename: string containing filename
    :return: list of file files
    """
    current_datetime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    full_filename = "{}-{}.png".format(filename, current_datetime)
    export_paths = file_export_path_list+[file_export_path_graphs]
    full_filepath = get_full_file_path(export_paths, full_filename)  # assume files are in specific directory
    try:
        pandas_plot.figure.savefig(full_filepath)
        return True
    except Exception as instance:
        return str(instance)
