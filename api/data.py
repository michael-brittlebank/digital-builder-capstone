from flask import make_response
from flask_restx import inputs, Namespace, reqparse, Resource
from modules.enums import *
from modules.database import create_application_tables, insert_housing_types, calculate_metrics, calculate_average_zhvi, select_location_densities
from modules.data import ingest_zillow_csv, calculate_agency_density

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
        housing_type = args[arg_housing_type]
        filename = args[arg_ingest_filename]
        filtered_data_length = ingest_zillow_csv(housing_type, filename)
        return filtered_data_length


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
        calculate_agency_density()
        return make_response('', 204)
