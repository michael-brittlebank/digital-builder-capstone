from ..database import calculate_average_zhvi


def calculate_yearly_zhvi():
    # calculate for all states
    calculate_average_zhvi(False)
    # calculate amfam only
    calculate_average_zhvi(True)
