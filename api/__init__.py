#https://flask-restx.readthedocs.io/en/latest/scaling.html

from flask_restx import Api

from .data import api as ns_data
from .analysis import api as ns_analysis
from .locations import api as ns_locations
from .graphs import api as ns_graphs

api = Api(
    title='CAP',
    version='1.0',
    description='Continuous Analysis Pipeline',
    # All API metadatas
)

namespaces = [ns_analysis, ns_data, ns_graphs, ns_locations]

for namespace in namespaces:
    api.add_namespace(namespace)
