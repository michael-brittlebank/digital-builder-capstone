from flask import Flask
from api import api
from dotenv import load_dotenv
import atexit
import logging
logging.basicConfig(filename='app.log', level=logging.WARNING, force=True)

load_dotenv()
app = Flask(__name__)
api.init_app(app)

@atexit.register
def shutdown():
    # shutdown hook
    pass


if __name__ == "__main__":
    app.run(debug=True)
