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
		filedata = ingest_zillow_csv(request.json['filename'])
		return filedata[0:25]





#https://www.codementor.io/@sagaragarwal94/building-a-basic-restful-api-in-python-58k02xsiq
#https://stackoverflow.com/questions/7907596/json-dumps-vs-flask-jsonify