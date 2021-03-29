from flask_restx import Namespace, Resource

api = Namespace('data', description='Data related operations')

@api.route('/')
class DataClass(Resource):
	def get(self):
		return {
			"status": "Got new data"
		}
	def post(self):
		return {
			"status": "Posted new data"
		}