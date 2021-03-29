#https://flask-restx.readthedocs.io/en/latest/scaling.html

from flask_restx import Api

from .data import api as ns1
from .graphs import api as ns2

api = Api(
    title='CAP',
    version='1.0',
    description='Continuous Analysis Pipeline',
    # All API metadatas
)

api.add_namespace(ns1)
api.add_namespace(ns2)