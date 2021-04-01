from flask import make_response
from flask_restx import inputs, Namespace, reqparse, Resource
from modules.graphs import *
from modules.enums import *

api = Namespace('graphs', description='Graph related operations', validate=True)

baseline_parser = reqparse.RequestParser()
baseline_parser.add_argument(
    arg_baseline_amfam_only,
    type=inputs.boolean,
    required=True,
    default=True,
    help='Return data for AmFam operating states only'
)


@api.route('/baseline')
class IngestClass(Resource):
    @api.expect(baseline_parser)
    def post(self):
        headers = {'Content-Type': 'text/csv'}
        args = baseline_parser.parse_args()
        data = get_baseline_graphs(args[arg_baseline_amfam_only])
        return make_response(data.to_csv(), 200, headers)
