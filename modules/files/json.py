from os import path
import json

from ._helpers import *
from ..enums import *


def import_json(filename):
    """
    ingest_json tries to import and read a json file
    :param filename: string containing filename
    :return: list of file files
    """
    full_filepath = get_full_file_path(file_import_path_list, filename)  # assume files are in specific directory
    try:
        if path.isfile(full_filepath):
            file = open(full_filepath, encoding="utf-8")
            raw_data = file.read()
            file.close()
            return json.loads(raw_data)
        else:
            raise Exception(FileNotFoundError, full_filepath + ' is not found')
    except Exception as instance:
        return str(instance)


def export_json(data, filename, export_path=file_export_path_json):
    """
    export_json tries to export a json file to the local directory
    :param data: json data to be written to file
    :param filename: string containing filename
    :param export_path: string for specified output folder
    :return: list of file files
    """
    export_path_list = file_export_path_list + export_path
    full_filepath = get_full_file_path(export_path_list, filename)  # assume files are in specific directory
    try:
        file = open(full_filepath, "w", encoding="utf-8")
        file.write(data)
        file.close()
        return True
    except Exception as instance:
        return str(instance)
