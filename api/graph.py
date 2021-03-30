from flask_restx import Namespace, Resource

api = Namespace('graph', description='Graph related operations', validate=True)

@api.route('/')
class GraphClass(Resource):
	def get(self):
		return {
			"status": "Got new grapb"
		}