import googlemaps
import time
import logging

from .geocode import *
from ._helpers import *
from ..files import *


def get_amfam_locations_by_zipcode(zipcode, radius=10):
    """
    get_amfam_locations gets a list of amfam locations within the given radius of a zipcode
    :param zipcode: int or string
    :param radius: int
    :return: list of amfam locations
    """
    # https://developers.google.com/maps/documentation/places/web-service/search#PlaceSearchRequests
    # https://github.com/googlemaps/google-maps-services-python/blob/master/tests/test_places.py
    location = get_location_for_zipcode(zipcode)
    result_count = 0
    if location[location_latitude] is not None:
        google_api_key = os.getenv(env_google_api_key)
        google_maps = googlemaps.Client(key=google_api_key)
        search_arguments = {
            "location": location,
            "radius": str(convert_miles_to_meters(radius)),
            "keyword": "american family",
            "type": ["insurance_agency"]
        }
        method_arguments = {
            places_zipcode: zipcode,
            places_counter: 0,
            places_amfam_count: 0
        }
        result_count = get_nearby_places(google_maps, search_arguments, method_arguments)
    else:
        logging.error("Location not found: %s", location)
        pass
    return result_count


def get_nearby_places(maps_client, search_arguments, method_arguments):
    """
    get_nearby_places recursively gets list of nearby results
    :param maps_client: google maps client initialised with api key
    :param search_arguments: google maps places_nearby arguments
    :param method_arguments: recursive arguments specific to method
    :return: count of amfam locations by zipcode
    """
    places_result = maps_client.places_nearby(**search_arguments)  # splat * list to create arguments
    method_arguments[places_amfam_count] += len(places_result[places_results])
    zipcode = method_arguments[places_zipcode]
    counter = method_arguments[places_counter]
    filename = "{}.json".format(zipcode)
    if counter > 0:
        filename = "{}-{}.json".format(zipcode, counter)
    logging.warning("get_nearby_places - in try: %s", filename)
    export_json(json.dumps(places_result), filename, file_export_path_places)
    if places_next_page_token in places_result:
        search_arguments[places_page_token] = places_result[places_next_page_token]  # set page token for more results
        method_arguments[places_counter] += 1  # increment counter for file export
        time.sleep(2)  # google needs to sleep for a little bit before the page token is available
        return get_nearby_places(maps_client, search_arguments, method_arguments)
    else:
        return method_arguments[places_amfam_count]
