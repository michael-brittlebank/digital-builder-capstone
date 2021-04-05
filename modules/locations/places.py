import googlemaps

from .geocode import *
from ._helpers import *
from ..enums import *


def get_amfam_locations_by_zipcode(zipcode, radius=10):
    """
    get_amfam_locations gets a list of amfam locations within the given radius of a zipcode
    :param zipcode: int or string
    :param radius: int
    :return: list of amfam locations
    """
    # https://developers.google.com/maps/documentation/places/web-service/search#PlaceSearchRequests
    # https://github.com/googlemaps/google-maps-services-python/blob/master/tests/test_places.py
    places_result = None
    location = get_location_for_zipcode(zipcode)
    if location[location_latitude] is not None:
        google_api_key = os.getenv('GOOGLE_API_KEY')
        gmaps = googlemaps.Client(key=google_api_key)
        places_result = gmaps.places_nearby(
                location=location,
                radius=str(convert_miles_to_meters(radius)),
                keyword="american family",
                type=["insurance_agency"]
            )

    else:
        # todo, log something here to catch parsing errors
        pass
    return places_result
