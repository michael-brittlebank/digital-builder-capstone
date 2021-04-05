import csv
from os import path
from ._helpers import *


def import_json(filename):
    """
    ingest_json tries to import and read a json file
    :param filename: string containing filename
    :return: list of file files
    """
    full_filepath = get_full_file_path(get_file_input_path(), filename)  # assume files are in specific directory
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


def export_csv(data, filename):
    """
    export_json tries to export a json file to the local directory
    :param data: json data to be written to file
    :param filename: string containing filename
    :return: list of file files
    """
    full_filepath = get_full_file_path(get_file_input_path(), filename)  # assume files are in specific directory
    try:
        with open(full_filepath, 'w') as f:
            # using csv.writer method from CSV package
            write = csv.writer(f)
            write.writerows(data_list)
        return True
    except Exception as instance:
        return str(instance)
