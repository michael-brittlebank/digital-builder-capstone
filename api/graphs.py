import datetime

from flask import make_response
from flask_restx import inputs, Namespace, reqparse, Resource
from modules.enums import *
from modules.graphs import get_baseline_graphs
from modules.files import export_dataframe_to_csv

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
    def get(self):
        args = baseline_parser.parse_args()
        amfam_only = args[arg_baseline_amfam_only]
        # get data
        data = get_baseline_graphs(amfam_only)
        # export data
        current_datetime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        data_type = "raw"
        if amfam_only:
            data_type = "amfamOnly"
        export_dataframe_to_csv(data, "{}-{}-{}.csv".format("summary", data_type, current_datetime))
        # response
        headers = {'Content-Type': 'text/csv'}
        return make_response(data.to_csv(), 200, headers)
