from flask import make_response
from flask_restx import Namespace, reqparse, Resource
from modules.files import *
from modules.enums import *

api = Namespace('files', description='File related operations', validate=True)

ingest_parser = reqparse.RequestParser()
ingest_parser.add_argument(
    arg_ingest_filename,
    required=True,
    help='The filename to be csv',
    trim=True
)
ingest_parser.add_argument(
    arg_ingest_type,
    required=True,
    choices=(zillow_data_type_condo, zillow_data_type_sfr),
    help='The type of files being uploaded',
    default=zillow_data_type_condo
)


@api.route('/ingest')
class IngestClass(Resource):
    @api.expect(ingest_parser)
    def post(self):
        args = ingest_parser.parse_args()
        data_type = args[arg_ingest_type]
        raw_data = import_csv(args[arg_ingest_filename])
        filtered_data = ingest_zillow_data(raw_data, data_type)
        debug_mode = os.getenv(env_flask_debug_mode)
        if bool(debug_mode):
            export_csv(filtered_data, 'ingested-{type}.csv'.format(type=data_type.lower()), file_export_path_testing)
        return len(filtered_data)


@api.route('/populate')
class PopulateClass(Resource):
    def post(self):
        create_application_tables()
        populate_housing_types()
        return make_response('', 204)
