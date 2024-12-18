from flask import request, jsonify, abort
from flight_planner.services import CityService, AirportService, FlightService

def register_routes(app):
    # City endpoints
    @app.route('/cities/', methods=['POST', 'GET'])
    def manage_cities():
        if request.method == 'POST':
            return jsonify(CityService.create_city(request.json)), 201
        elif request.method == 'GET':
            try:
                return jsonify(CityService.get_all_cities())
            except Exception:
                abort(404)

    @app.route('/cities/<int:city_id>', methods=['GET', 'DELETE'])
    def city_detail(city_id):
        if request.method == 'GET':
            try:
                return jsonify(CityService.get_city(city_id))
            except Exception:
                abort(404)
        elif request.method == 'DELETE':
            return jsonify(CityService.delete_city(city_id)), 204

    @app.route('/cities/delete_all', methods=['DELETE'])
    def delete_all_cities():
        return jsonify(CityService.delete_all_cities()), 204

    @app.route('/airports/delete_all', methods=['DELETE'])
    def delete_all_airports():
        return jsonify(AirportService.delete_all_airports()), 204

    @app.route('/flights/delete_all', methods=['DELETE'])
    def delete_all_flights():
        return jsonify(FlightService.delete_all_flights()), 204
    # Airport endpoints
    @app.route('/airports/', methods=['POST', 'PUT', 'GET'])
    def manage_airports():
        if request.method == 'POST':
            return jsonify(AirportService.create_airport(request.json)), 201
        elif request.method == 'PUT':
            return jsonify(AirportService.update_all_airports(request.json))
        elif request.method == 'GET':
            try:
                return jsonify(AirportService.get_all_airports())
            except Exception:
                abort(404)

    @app.route('/airports/<int:airport_id>', methods=['GET', 'PUT', 'DELETE'])
    def airport_detail(airport_id):
        if request.method == 'GET':
            try:
                return jsonify(AirportService.get_airport(airport_id))
            except Exception:
                abort(404)
        elif request.method == 'PUT':
            return jsonify(AirportService.update_airport(airport_id, request.json))
        elif request.method == 'DELETE':
            return jsonify(AirportService.delete_airport(airport_id))

    # Flight endpoints
    @app.route('/flights/', methods=['POST', 'GET'])
    def manage_flights():
        if request.method == 'POST':
            return jsonify(FlightService.create_flight(request.json)), 201
        elif request.method == 'GET':
            offset = request.args.get('offset', default=0, type=int)
            max_count = request.args.get('maxCount', default=50, type=int)
            sort_by = request.args.get('sortBy', default='departureTime', type=str)
            sort_order = request.args.get('sortOrder', default='ASC', type=str)
            return jsonify(FlightService.get_all_flights(offset, max_count, sort_by, sort_order))

    @app.route('/flights/search', methods=['POST'])
    def search_flights():
        search_criteria = request.json
        return jsonify(FlightService.search_flights(search_criteria))

    @app.route('/flights/<int:flight_id>', methods=['GET', 'PUT', 'DELETE'])
    def flight_detail(flight_id):
        if request.method == 'GET':
            return jsonify(FlightService.get_flight(flight_id))
        elif request.method == 'PUT':
            return jsonify(FlightService.update_flight(flight_id, request.json))
        elif request.method == 'DELETE':
            return jsonify(FlightService.delete_flight(flight_id))



