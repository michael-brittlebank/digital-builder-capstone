import copy
import pandas as pd
from ..enums import *


def ingest_zillow_data(raw_data, data_type):
    """
    normalise_zillow_data translates csv files into analysable files
    :param raw_data: recently imported files from csv
    :param data_type: type of csv being imported
    :return: list of filtered files
    """
    filtered_data = []
    county_name_index = zillow_named_column_indexes.index(zillow_column_county_name)
    header_row = raw_data.pop(0)
    original_header = copy.deepcopy(header_row)
    header_row = header_row[:county_name_index+1] # slice off date columns, handled in inflate_zillow_row_by_date, +1 for inclusive slice
    header_row += [custom_column_housing_type, custom_column_date, custom_column_zhvi] # add new inflated column headers
    for row in raw_data[0:50]:  # todo, remove slice to get full import
        normalised_row = normalise_zillow_row(row)
        inflated_rows = inflate_zillow_row_by_date(normalised_row, original_header, data_type)
        filtered_data += inflated_rows
    return [header_row] + filtered_data


def normalise_zillow_row(row):
    """
    normalise_zillow_row normalises zillow row to be consumed by downstream processes
    :param row: files from zillow row to be normalised
    :return: list of normalised row
    """
    size_rank_index = zillow_named_column_indexes.index(zillow_column_size_rank)
    county_name_index = zillow_named_column_indexes.index(zillow_column_county_name)
    normalised_row = []
    for index in range(len(row)):
        row_value = row[index]
        if index <= size_rank_index:
            # int, size rank and id
            row_value = int(row_value)
        elif index <= county_name_index:
            # strings
            row_value = str(row_value)
        else:
            # assume 0 for empty floats
            if row_value == '':
                row_value = 0
            else:
                row_value = float(row_value)
        normalised_row.append(row_value)
    return normalised_row


def inflate_zillow_row_by_date(row, header, data_type):
    """
    inflate_zillow_row_by_date inflates a single zillow row into multiple rows by date to create a long table from a wide table
    :param row: files from zillow row to be normalised
    :param header: row header columns
    :param data_type: type of files row being normalised
    :return: list of inflated rows
    """
    inflated_rows = []
    # +1 for exclusive range and inclusive slice
    county_name_index = zillow_named_column_indexes.index(zillow_column_county_name) + 1
    common_row_metadata = row[:county_name_index]  # everything to housing type is common
    for index in range(county_name_index, len(header)):
        common_copy = copy.deepcopy(common_row_metadata)
        common_copy.append(data_type) # add the files type for differentiating condos and single family residences
        converted_header_to_timestamp = str(pd.to_datetime(header[index]))
        common_copy.append(converted_header_to_timestamp)  # add header date
        common_copy.append(row[index])  # housing value at date
        inflated_rows.append(common_copy)
    return inflated_rows
