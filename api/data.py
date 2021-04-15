import os

from flask import make_response
from flask_restx import inputs, Namespace, reqparse, Resource
from modules.files import *
from modules.database import create_application_tables, insert_housing_types, calculate_metrics, calculate_average_zhvi, calculate_density

api = Namespace('data', description='Data related operations', validate=True)

ingest_parser = reqparse.RequestParser()
ingest_parser.add_argument(
    arg_ingest_filename,
    required=True,
    help='The filename to be csv',
    trim=True
)
ingest_parser.add_argument(
    arg_housing_type,
    required=True,
    choices=(zillow_data_type_condo, zillow_data_type_sfr),
    help='The type of files being uploaded',
    default=zillow_data_type_condo
)

calculate_parser = reqparse.RequestParser()
calculate_parser.add_argument(
    arg_baseline_amfam_only,
    type=inputs.boolean,
    required=True,
    default=True,
    help='Calculate analysis for AmFam operating states only'
)


@api.route('/ingest')
class IngestClass(Resource):
    @api.expect(ingest_parser)
    def post(self):
        args = ingest_parser.parse_args()
        data_type = args[arg_housing_type]
        filename = args[arg_ingest_filename]
        raw_data = import_csv(filename)
        filtered_data = ingest_zillow_data(raw_data, data_type)
        debug_mode = os.getenv(env_flask_debug_mode)
        if bool(debug_mode):
            export_csv(filtered_data, 'ingested-{type}.csv'.format(type=data_type.lower()), file_export_path_testing)
        return len(filtered_data)


@api.route('/populate')
class PopulateClass(Resource):
    def post(self):
        create_application_tables()
        insert_housing_types()
        return make_response('', 204)


@api.route('/calculate-location-metrics')
class CalculateMetricsClass(Resource):
    def post(self):
        calculate_metrics()
        return make_response('', 204)


@api.route('/calculate-yearly-zhvi')
class CalculateZhviClass(Resource):
    @api.expect(calculate_parser)
    def post(self):
        args = calculate_parser.parse_args()
        amfam_only = args[arg_baseline_amfam_only]
        calculate_average_zhvi(amfam_only)
        return make_response('', 204)


@api.route('/calculate-agency-density')
class CalculateDensityClass(Resource):
    def post(self):
        calculate_density()
        return make_response('', 204)