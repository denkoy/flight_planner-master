from flask import Flask
from flight_planner.routes import register_routes

def create_app():
    app = Flask(__name__)
    register_routes(app)
    return app

def main():
    app = create_app()
    app.run(debug=True)

if __name__ == '__main__':
    main()