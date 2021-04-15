import pandas as pd

from ..enums import *
from ..analysis import get_leaders_data
from ..locations import get_amfam_locations_by_zipcode


def calculate_agency_density():
    is_only_amfam_data = True # limit due to google api costs
    is_raw_data = True
    # get condo leaders
    condo_leaders = get_leaders_data(is_only_amfam_data, zillow_data_type_condo, is_raw_data)
    # get sfr leaders
    sfr_leaders = get_leaders_data(is_only_amfam_data, zillow_data_type_sfr, is_raw_data)
    # combine to same list
    leaders_dataframe = pd.concat([condo_leaders, sfr_leaders])
    # get unique zipcodes in leaders list
    zipcodes = []
    for index, row in leaders_dataframe.iterrows():
        zipcodes.append(row[zillow_column_region_name])
    zipcodes = list(set(zipcodes))
    for zipcode in zipcodes:
        radius = 10  # miles
        get_amfam_locations_by_zipcode(zipcode, radius)
    return True
