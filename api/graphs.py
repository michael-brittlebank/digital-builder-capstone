from datetime import datetime

from flask import make_response
from flask_restx import Namespace, Resource
from modules.graphs import get_zhvi_trend_graph
from modules.files import export_dataframe_to_csv

api = Namespace('graphs', description='Graph related operations', validate=True)


@api.route('/trend-zhvi')
class TrendClass(Resource):
    def get(self):
        # get data
        data = get_zhvi_trend_graph()
        # export data
        current_datetime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        export_dataframe_to_csv(data, "{}-{}.csv".format("zhvi-trend", current_datetime))
        # response
        headers = {'Content-Type': 'text/csv'}
        return make_response(data.to_csv(), 200, headers)
