from flask_restx import inputs, Namespace, reqparse, Resource
from modules.data import *

api = Namespace('data', description='Data related operations', validate=True)

base_parser = reqparse.RequestParser()
base_parser.add_argument(
    baseline_arg_amfam_only,
	type=inputs.boolean,
	required=True,
	default=True,
    help='Return data for AmFam operating states only'
)

@api.route('/baseline')
class BaselineGraphClass(Resource):
	@api.expect(base_parser)
	def get(self):
		args = base_parser.parse_args()
		return get_baseline_data(args[baseline_arg_amfam_only])