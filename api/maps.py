from flask_restx import Namespace, reqparse, Resource
from modules.data import *
from modules.maps import *

api = Namespace('maps', description='Map related operations', validate=True)

locations_parser = reqparse.RequestParser()
locations_parser.add_argument(
    arg_locations_latitude,
	required=True,
    help='Latitude'
)
locations_parser.add_argument(
    arg_locations_longitude,
	required=True,
    help='Longititude'
)

@api.route('/locations')
class LocationsClass(Resource):
    @api.expect(locations_parser)
    def post(self):
        args = locations_parser.parse_args()
        latitude = args[arg_locations_latitude]
        longitude = args[arg_locations_longitude]
        return get_latitude_longitude_for_zip('53545')