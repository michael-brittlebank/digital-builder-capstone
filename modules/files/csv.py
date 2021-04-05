import csv
from os import path

from ._helpers import *
from ..enums import *


def import_csv(filename, import_path=file_import_path_list):
    """
    ingest_csv tries to import and read a csv file
    :param filename: string containing filename
    :param import_path: string for specified input folder
    :return: list of data
    """
    full_filepath = get_full_file_path(import_path, filename)  # assume files are in specific directory
    try:
        if path.isfile(full_filepath):
            # copied from https://automatetheboringstuff.com/chapter14/
            file = open(full_filepath, encoding="utf-8")
            reader = csv.reader(file)
            data = list(reader)
            file.close()
            return data
        else:
            raise Exception(FileNotFoundError, full_filepath + ' is not found')
    except Exception as instance:
        return str(instance)


def export_csv(data_list, filename, export_path=file_export_path_csv):
    """
    export_csv tries to export a csv file to the local directory
    :param data_list: list of data to be written to file
    :param filename: string containing filename
    :param export_path: string for specified output folder
    :return: boolean or string exception
    """
    export_path_list = file_export_path_list + [export_path]
    full_filepath = get_full_file_path(export_path_list, filename)  # assume files are in specific directory
    try:
        with open(full_filepath, 'w') as file:
            # using csv.writer method from CSV package
            write = csv.writer(file)
            write.writerows(data_list)
        return True
    except Exception as instance:
        return str(instance)


def export_dataframe_to_csv(dataframe, filename, export_path=file_export_path_csv):
    """
    export_dataframe_to_csv exports a pandas dataframe to the local directory as a csv
    :param dataframe: pandas dataframe to be written to file
    :param filename: string containing filename
    :param export_path: string for specified output folder
    :return: boolean
    """
    export_path_list = file_export_path_list + [export_path]
    full_filepath = get_full_file_path(export_path_list, filename)  # assume files are in specific directory
    dataframe.to_csv(path_or_buf=full_filepath)
    return True
