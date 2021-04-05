import os
from flask import Flask
from api import api
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
api.init_app(app)

if __name__ == "__main__":
    app.run(debug=True)
