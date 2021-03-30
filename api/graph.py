from flask_restx import inputs, Namespace, reqparse, Resource
from modules.graph import *
from modules.util import *

api = Namespace('graph', description='Graph related operations', validate=True)

base_parser = reqparse.RequestParser()
base_parser.add_argument(
    baseline_arg_amfam_only,
	type=inputs.boolean,
	required=True,
	default=True,
    help='Select baseline data for AmFam operating states only'
)

@api.route('/baseline')
class BaselineGraphClass(Resource):
	@api.expect(base_parser)
	def get(self):
		args = base_parser.parse_args()
		return get_baseline_graph(args[baseline_arg_amfam_only])