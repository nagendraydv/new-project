
"""
Main module of the server file
"""

# 3rd party moudles
from flask import render_template
import connexion
from connexion import RestyResolver

# Create the application instance
app = connexion.App(__name__, specification_dir="./")

# Read the swagger.yml file to configure the endpoints
app.add_api("swagger.yml")

if __name__ == "__main__":
    app.run(debug=True,port=8080)
