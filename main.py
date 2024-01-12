from api import create_app
from flask_cors import CORS, cross_origin
from flask import Flask, render_template
import os
import random
import threading
import time

app = create_app()
CORS(app)


@app.route("/")
def hello_world():
  """ URL returns random number """
  return str(number)


def gen_rand():
  """ generating random numbers """
  global number
  while True:
    number = random.randint(0, 5)
    time.sleep(2)


if __name__ == '__main__':
  x = threading.Thread(target=gen_rand, daemon=True)
  x.start()
  from waitress import serve
  serve(app, host="0.0.0.0", port=int(os.environ.get('PORT', 8080)))
