import os

from ..enums import *
from ..files import import_csv, export_csv, ingest_zillow_data


def ingest_zillow_csv(housing_type, filename):
    raw_data = import_csv(filename)
    filtered_data = ingest_zillow_data(raw_data, housing_type)
    debug_mode = os.getenv(env_flask_debug_mode)
    if bool(debug_mode):
        export_csv(filtered_data, 'ingested-{type}.csv'.format(type=housing_type.lower()), file_export_path_testing)
    return len(filtered_data)