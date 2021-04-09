from flask import Flask
from api import api
from dotenv import load_dotenv
import atexit
import os
import logging

logging.basicConfig(filename='app.log', level=logging.INFO, force=True, datefmt='%Y-%m-%d %H:%M:%S')

from modules.enums.env import env_flask_debug_mode

load_dotenv()
app = Flask(__name__)
api.init_app(app)


@atexit.register
def shutdown():
    # shutdown hook
    pass


if __name__ == "__main__":
    app.run(debug=bool(os.getenv(env_flask_debug_mode)))
