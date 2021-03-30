from flask import request
from flask_restx import fields, Namespace, Resource
from modules.data import *

api = Namespace('data', description='Data related operations')

file = api.model('File', {
    'filename': fields.String(required=True, description='The filename to be ingested')
})

@api.route('/ingest')
class DataClass(Resource):
	def post(self):
		raw_data = ingest_zillow_csv(request.json['filename'])
		filtered_data = filter_normalise_zillow_data(raw_data)
		# todo, store results in db
		return filtered_data





#https://www.codementor.io/@sagaragarwal94/building-a-basic-restful-api-in-python-58k02xsiq
#https://stackoverflow.com/questions/7907596/json-dumps-vs-flask-jsonify