import os
from ..enums import *


def get_file_input_path():
    """
    get_file_input_path returns os-specific path to input files
    :return: file path string
    """
    return os.path.join(file_path, file_input_path)


def get_file_output_path():
    """
    get_file_output_path returns os-specific path to output files
    :return: file path string
    """
    return os.path.join(file_path, file_output_path)


def get_full_file_path(file_path, file_name):
    """
    get_full_file_path returns os-specific path for a given filename nad path
    :param file_path: file path
    :param file_name: file name
    :return: file path string
    """
    return os.path.join(file_path, file_name)
