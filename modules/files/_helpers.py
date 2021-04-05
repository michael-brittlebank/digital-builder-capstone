import os


def get_full_file_path(file_path_list, file_name):
    """
    get_full_file_path returns os-specific path for a given filename and path
    :param file_path_list: file path
    :param file_name: file name
    :return: file path string
    """
    file_path_list.append(file_name)
    return os.path.join(file_path_list)
