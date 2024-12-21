# Flight Planner

A simple flight planning application with RESTful API endpoints for managing cities, airports, flights, and connecting flights.

## Features

- Manage cities, airports, flights, and connecting flights.
- Supports data persistence in files or an SQL database.
- RESTful API with endpoints for CRUD operations.

## Requirements for student implementation:
* You are allowed to edit all files, except `flight_planner/app.py`, and to a much lesser degree - `flight_planner/app.py`.

Make sure to preserve the API and most of `flight_planner/routes.py` - edit to fit your own services implementation.

*Note*: The `test_services.py` file provides basic logic tests for your services implementation. Your code should pass these tests if it is valid.

`Dockerfile` and `setup.py` are provided optionally.

Basic points: 15pts (For correct implementation of the base task)

### Bonus points:
* For importing data from a CSV file (no export) (+1 pt)
* For file-based storage (both loading and saving) (+2 pt)
* For SQL-based storage (both loading and saving) (+2 pt)
* For correct and passing unit tests (test_services.py) (+1 pt)

**NOTE**: In case you're providing multiple storage mechanisms, your services layer should select the appropriate data store (In memory / File-based / SQL-based) based on the value of the `DATA_STORE` environment variable.
If `DATA_STORE` is not set, or `inmemory`, use the In Memory store. If it is equal to `file`, use the File-based store. If it is equal to `sql`, use the SQL-based store.

**NOTE**: Your data stores should have only optional arguments, thus allowing instantiation without any arguments.


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
- `DELETE /airports/` - Delete all airports
- `GET /airports/` - Get a collection of airports
- `GET /airports/<id>` - Get an airport by ID
- `DELETE /airports/<id>` - Delete an airport by ID

### Flight Endpoints
- `POST /flights/` - Create a new flight
  - Request body example:
    ```json
    {
      "arrivalAirport": 27,
      "departureAirport": 2,
      "departureTime": "12:35",
      "travelTime": 45,
      "price": "$300"
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

### Explanation:

- **Directory Structure**: The project is organized into a package named `flight_planner` with submodules for different components like models, routes, services, and persistence.

- **Persistence Options**: Two persistence options are provided: `FileStorage` for file-based storage and `SQLStorage` for SQLite database storage.

- **Virtual Environment**: The `requirements.txt` file lists the dependencies needed for the project.
