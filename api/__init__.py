#https://flask-restx.readthedocs.io/en/latest/scaling.html

from flask_restx import Api

from .files import api as ns_files
from .data import api as ns_data
from .maps import api as ns_maps
from .graphs import api as ns_graphs

api = Api(
    title='CAP',
    version='1.0',
    description='Continuous Analysis Pipeline',
    # All API metadatas
)

api.add_namespace(ns_files)
api.add_namespace(ns_data)
api.add_namespace(ns_maps)
api.add_namespace(ns_graphs)
