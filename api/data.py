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





#https://www.codementor.io/@sagaragarwal94/building-a-basic-restful-api-in-python-58k02xsiq
#https://stackoverflow.com/questions/7907596/json-dumps-vs-flask-jsonify