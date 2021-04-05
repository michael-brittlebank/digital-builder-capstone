from flask_restx import Namespace, reqparse, Resource
from modules.locations import *

api = Namespace('locations', description='Locations related operations', validate=True)

locations_parser = reqparse.RequestParser()
locations_parser.add_argument(
    arg_locations_zipcode,
	required=True,
    help='Zip code'
)
locations_parser.add_argument(
    arg_locations_radius,
    help='Radius'
)

@api.route('/locations')
class LocationsClass(Resource):
    @api.expect(locations_parser)
    def post(self):
        args = locations_parser.parse_args()
        zipcode = args[arg_locations_zipcode]
        radius = 10
        if args[arg_locations_radius] is not None:
            radius = args[arg_locations_radius]
        return get_amfam_locations_by_zipcode(zipcode, radius)
