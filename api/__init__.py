from flask import Flask
from firebase_admin import credentials, initialize_app

cred = credentials.Certificate("api/key.json")

default_app = initialize_app(cred,
                             {'storageBucket': 'ssproject-376507.appspot.com'})


def create_app():
  app = Flask(__name__)
  app.config['SECRET_KEY'] = '12345rtfescdvf'
  from .movieAPI import movieAPI
  app.register_blueprint(movieAPI, url_prefix='/api/v1/movie')
  return app
