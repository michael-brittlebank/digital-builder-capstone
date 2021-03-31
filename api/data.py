from flask_restx import Namespace, reqparse, Resource
from modules.data import *
from modules.util import *

api = Namespace('data', description='Data related operations', validate=True)

ingest_parser = reqparse.RequestParser()
ingest_parser.add_argument(
    ingest_arg_filename,
	required=True,
    help='The filename to be ingested',
	trim=True
)
ingest_parser.add_argument(
    ingest_arg_type,
	required=True,
    choices=(zillow_data_type_condo, zillow_data_type_sfr),
    help='The type of data being uploaded',
	default=zillow_data_type_condo
)

@api.route('/ingest')
class DataClass(Resource):
	@api.expect(ingest_parser)
	def post(self):
		args = ingest_parser.parse_args()
		data_type = args[ingest_arg_type]
		raw_data = import_csv(args[ingest_arg_filename])
		filtered_data = ingest_zillow_data(raw_data, data_type)
		export_csv(filtered_data, 'ingested_{type}.csv'.format(type=data_type.lower()))
		# todo, store results in db
		return len(filtered_data)





#https://www.codementor.io/@sagaragarwal94/building-a-basic-restful-api-in-python-58k02xsiq
#https://stackoverflow.com/questions/7907596/json-dumps-vs-flask-jsonify