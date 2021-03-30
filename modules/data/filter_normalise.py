from ..util import *


def filter_normalise_zillow_data(raw_data):
    """
    filter_normalise_zillow_data filters zillow data by amfam locations
    :param raw_data: recently imported data from csv
    :return: list of filtered data
    """
    filtered_data = []
    state_index = zillow_named_column_indexes.index(zillow_column_state)
    for row in raw_data:
        row_state = row[state_index]
        if row_state != zillow_column_state and row_state in amfam_territory_states:
            # filter out if header column or not in amfam territory
            filtered_data.append(normalise_zillow_row(row))
    return filtered_data


def normalise_zillow_row(row):
    """
    normalise_zillow_row normalises zillow row to be consumed by downstream processes
    :param row: data from zillow row to be normalised
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
