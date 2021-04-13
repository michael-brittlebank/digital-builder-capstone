from flask_restx import Namespace, reqparse, Resource
from modules.enums import *
from modules.locations import get_amfam_locations_by_zipcode

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

@api.route('/nearby')
class LocationsClass(Resource):
    @api.expect(locations_parser)
    def get(self):
        args = locations_parser.parse_args()
        zipcode = args[arg_locations_zipcode]
        radius = 10
        if args[arg_locations_radius] is not None:
            radius = args[arg_locations_radius]
        return get_amfam_locations_by_zipcode(zipcode, radius)
