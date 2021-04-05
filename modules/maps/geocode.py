import os
import googlemaps

from ..enums import *


def get_latitude_longitude_for_zip(zipcode):
    location = {
        location_latitude: None,
        location_longitude: None
    }
    google_api_key = os.getenv('GOOGLE_API_KEY')
    gmaps = googlemaps.Client(key=google_api_key)
    geocode_result = gmaps.geocode(zipcode)
    try:
        if len(geocode_result) > 0:
            geocode_location = geocode_result[0]['geometry']['location']
            location[location_latitude] = geocode_location['lat']
            location[location_longitude] = geocode_location['lng']
    except:
        # log something here to catch parsing errors
        pass
    return location
