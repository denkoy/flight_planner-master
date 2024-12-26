# Flight Planner

A simple flight planning application with RESTful API endpoints for managing cities, airports, flights, and connecting flights.

## Features

- Manage cities, airports, flights, and connecting flights.
- Supports data persistence in files or an SQL database.
- RESTful API with endpoints for CRUD operations.

## API Endpoints

### City Endpoints
- `POST /cities/` - Create a new city
- `GET /cities/` - Get all cities
- `DELETE /cities/` - Delete all cities
- `GET /cities/<id>` - Get a city by ID
- `DELETE /cities/<id>` - Delete a city by ID

### Airport Endpoints
- `POST /airports/` - Create a new airport
- `PUT /airports/` - Edit the whole collection of airports
  - Request body example:
    ```json
      {
      "1":{"name":"Sofia airport","city_id":"1"},
      "2":{"name":"Pld Airport","city_id":"2"}
      }
    ```
- `PUT /airports/<id>` - Edit airport by ID
- `DELETE /airports/` - Delete all airports
- `GET /airports/` - Get a collection of airports
- `GET /airports/<id>` - Get an airport by ID
- `DELETE /airports/<id>` - Delete an airport by ID

### Flight Endpoints
- `POST /flights/` - Create a new flight
  - Request body example:
    ```json
    {
    "name":"FR3003",
    "arrivalAirport": "Sofia Airport",
    "departureAirport": "Plovdiv Airport",
    "departureTime": "12:35",
    "travelTime": 45,
    "price": "300"
    }
    ```
- `GET /flights/` - Get all flights with optional query parameters for pagination and sorting
  - Query parameters:
    - `offset` (default: 0) - The starting point for the list of flights
    - `maxCount` (default: 50) - The maximum number of flights to return
    - `sortBy` (default: "departureTime") - The field to sort by
    - `sortOrder` (default: "ASC") - The order of sorting (ASC or DESC)
- `POST /flights/search` - Search for flights based on criteria
  - Request body example:
    ```json
    {
      "departureCity": "New York",
      "arrivalCity": "Los Angeles",
      "minPrice": 100,
      "maxPrice": 500,
      "minDepartureTime": "08:00",
      "maxDepartureTime": "20:00",
      "maxTravelTime": 300,
      "minArrivalTime": "10:00",
      "maxArrivalTime": "22:00"
    }
    ```
- `GET /flights/<id>` - Get a flight by ID
- `PUT /flights/<id>` - Edit a flight by ID
- `DELETE /flights/<id>` - Delete a flight by ID

### Additional Features
- `SQL Storage` - All of the data used by the API is stored in SQL database. SQLite3 is used for easier management.
- `Flask` - Flask is used for the managing of the API service.
- `CSV service` - There is functionality which allows you to import data from a CSV file(ObjectService.initialize_database) method.
- `Tests` - I wrote around 60 tests which test all the functionalities of my project.
