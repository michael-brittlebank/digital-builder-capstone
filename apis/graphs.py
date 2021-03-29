from flask_restx import Namespace, Resource

api = Namespace('graphs', description='Graph related operations')

@api.route('/')
class DataClass(Resource):
	def get(self):
		return {
			"status": "Got new grapb"
		}