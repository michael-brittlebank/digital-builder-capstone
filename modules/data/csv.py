# imports
import csv
from os import path


# copied from https://automatetheboringstuff.com/chapter14/
def open_csv(filename):
    """
    open_csv imports a csv file and returns the file data
    :param filename: string containing filename and path
    :return: list of file data
    """
    file = open(filename, encoding="utf-8")
    reader = csv.reader(file)
    data = list(reader)
    file.close()
    return data


def import_zillow_csv(filename):
    """
    ingest_zillow_csv tries to import and read a zillow csv file
    :param filename: string containing filename
    :return: list of file data
    """
    full_filepath = 'files/'+filename #assume files are in specific directory
    try:
        if path.isfile(full_filepath):
            return open_csv(full_filepath)
        else:
            raise Exception(FileNotFoundError, full_filepath+' is not found')
    except Exception as instance:
        return str(instance)

