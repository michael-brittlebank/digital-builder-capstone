from ..enums import *
from ..database import calculate_average_zhvi


def calculate_yearly_zhvi():
    # calculate for all states
    calculate_average_zhvi(all_states_indicator)
    # calculate amfam only
    calculate_average_zhvi(amfam_only_states_indicator)
