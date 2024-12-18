import json
from database import get_db_connection
from flight_planner.app import create_app
import unittest
from unittest.mock import patch, MagicMock
from flight_planner.services import CityService, AirportService, FlightService, ObjectService


# FILE: flight_planner/test_services.py


class SmartTestCaseFromAbies(unittest.TestCase):
    def assertIncludes(self, actual, expected):
        for key, value in expected.items():
            self.assertEqual(actual[key], value)

    def assertPiecewiseIncludes(self, actual, expected):
        if len(actual) != len(expected):
            self.fail("Length mismatch")
        for (item, reference) in zip(actual, expected):
            self.assertIncludes(item, reference)


class TestCityServiceFromAbies(SmartTestCaseFromAbies):
    @patch("flight_planner.services.storage")
    def test_1_create_city(self, mock_storage):
        mock_storage.create_city.return_value = {"id": 1, "name": "Test City"}
        response = CityService.create_city({"name": "Test City"})
        self.assertIncludes(response, {"id": 1, "name": "Test City"})

    @patch("flight_planner.services.storage")
    def test_2_get_all_cities(self, mock_storage):
        mock_storage.get_all_cities.return_value = [{"id": 1, "name": "Test City"}]
        response = CityService.get_all_cities()
        self.assertPiecewiseIncludes(response, [{"id": 1, "name": "Test City"}])

    @patch("flight_planner.services.storage")
    def test_3_get_city(self, mock_storage):
        mock_storage.get_city.return_value = {"id": 1, "name": "Test City"}
        response = CityService.get_city(1)
        self.assertIncludes(response, {"id": 1, "name": "Test City"})

    @patch("flight_planner.services.storage")
    def test_6_get_city_not_found(self, mock_storage):
        mock_storage.get_city.return_value = None
        with self.assertRaises(KeyError) as context:
            CityService.get_city(1)

    @patch("flight_planner.services.storage")
    def test_4_delete_city(self, mock_storage):
        response = CityService.delete_city(1)
        self.assertEqual(response, "")

    @patch("flight_planner.services.storage")
    def test_5_delete_all_cities(self, mock_storage):
        response = CityService.delete_all_cities()
        self.assertEqual(response, "")


class TestAirportServiceFromAbies(SmartTestCaseFromAbies):
    @patch("flight_planner.services.storage")
    def test_1_create_airport(self, mock_storage):
        mock_storage.create_airport.return_value = {"id": 1, "name": "Test Airport"}
        response = AirportService.create_airport({"name": "Test Airport"})
        self.assertIncludes(response, {"id": 1, "name": "Test Airport"})

    @patch("flight_planner.services.storage")
    def test_2_get_all_airports(self, mock_storage):
        mock_storage.get_all_airports.return_value = [{"id": 1, "name": "Test Airport"}]
        response = AirportService.get_all_airports()
        self.assertPiecewiseIncludes(response, [{"id": 1, "name": "Test Airport"}])

    @patch("flight_planner.services.storage")
    def test_3_get_airport(self, mock_storage):
        mock_storage.get_airport.return_value = {"id": 1, "name": "Test Airport"}
        response = AirportService.get_airport(1)
        self.assertIncludes(response, {"id": 1, "name": "Test Airport"})

    @patch("flight_planner.services.storage")
    def test_6_get_airport_not_found(self, mock_storage):
        mock_storage.get_airport.return_value = None
        with self.assertRaises(KeyError) as context:
            AirportService.get_airport(1)

    @patch("flight_planner.services.storage")
    def test_4_delete_airport(self, mock_storage):
        response = AirportService.delete_airport(1)
        self.assertEqual(response, "")

    @patch("flight_planner.services.storage")
    def test_5_delete_all_airports(self, mock_storage):
        response = AirportService.delete_all_airports()
        self.assertEqual(response, "")


class TestFlightServiceFromAbies(SmartTestCaseFromAbies):
    @patch("flight_planner.services.storage")
    def test_1_create_flight(self, mock_storage):
        mock_storage.create_flight.return_value = {"id": 1, "name": "Test Flight"}
        response = FlightService.create_flight({"name": "Test Flight"})
        self.assertIncludes(response, {"id": 1, "name": "Test Flight"})

    @patch("flight_planner.services.storage")
    def test_2_get_all_flights(self, mock_storage):
        mock_storage.get_all_flights.return_value = [{"id": 1, "name": "Test Flight"}]
        response = FlightService.get_all_flights()
        self.assertPiecewiseIncludes(response, [{"id": 1, "name": "Test Flight"}])

    @patch("flight_planner.services.storage")
    def test_3_get_flight(self, mock_storage):
        mock_storage.get_flight.return_value = {"id": 1, "name": "Test Flight"}
        response = FlightService.get_flight(1)
        self.assertIncludes(response, {"id": 1, "name": "Test Flight"})

    @patch("flight_planner.services.storage")
    def test_6_get_flight_not_found(self, mock_storage):
        mock_storage.get_flight.return_value = None
        with self.assertRaises(KeyError) as context:
            FlightService.get_flight(1)

    @patch("flight_planner.services.storage")
    def test_4_delete_flight(self, mock_storage):
        response = FlightService.delete_flight(1)
        self.assertEqual(response, "")

    @patch("flight_planner.services.storage")
    def test_5_delete_all_flights(self, mock_storage):
        response = FlightService.delete_all_flights()
        self.assertEqual(response, "")


#########################################


# My tests


class TestCityService(unittest.TestCase):
    @patch("flight_planner.services.storage")
    def test_a_create_city(self, mock_storage):
        mock_storage.create_city.return_value = {"id": 1, "name": "Test City"}
        response = CityService.create_city({"name": "Test City"})
        self.assertEqual(response, {"id": 1, "name": "Test City"})

    @patch("flight_planner.services.storage")
    def test_b_get_all_cities(self, mock_storage):
        mock_storage.get_all_cities.return_value = [{"id": 1, "name": "Test City"}]
        response = CityService.get_all_cities()
        self.assertEqual(response, [{"id": 1, "name": "Test City"}])

    @patch("flight_planner.services.storage")
    def test_c_get_city(self, mock_storage):
        mock_storage.get_city.return_value = {"id": 1, "name": "Test City"}
        response = CityService.get_city(1)
        self.assertEqual(response, {"id": 1, "name": "Test City"})

    @patch("flight_planner.services.storage")
    def test_g_get_city_not_found(self, mock_storage):
        mock_storage.get_city.return_value = None
        with self.assertRaises(KeyError) as context:
            CityService.get_city(1)
        tear_down_module()

    @patch("flight_planner.services.storage")
    def test_e_delete_a_city(self, mock_storage):
        response = CityService.delete_city(1)
        self.assertEqual(response, "")

    @patch("flight_planner.services.storage")
    def test_f_delete_b_all_cities(self, mock_storage):
        response = CityService.delete_all_cities()
        self.assertEqual(response, "")


class TestAirportService(unittest.TestCase):
    @patch("flight_planner.services.storage")
    def test_a_create_airport(self, mock_storage):
        mock_storage.create_airport.return_value = {"id": 1, "name": "Test Airport"}
        CityService.create_city({"name": "Sofia de Janeiro"})
        response = AirportService.create_airport({"name": "Test Airport", "city_id": 1})
        self.assertEqual(response, {"id": 1, "name": "Test Airport", "city_id": 1})

    @patch("flight_planner.services.storage")
    def test_b_get_all_airports(self, mock_storage):
        mock_storage.get_all_airports.return_value = [{"id": 1, "name": "Test Airport"}]
        response = AirportService.get_all_airports()
        self.assertEqual(response, [{"id": 1, "name": "Test Airport", "city_id": 1}])

    @patch("flight_planner.services.storage")
    def test_c_get_airport(self, mock_storage):
        mock_storage.get_airport.return_value = {"id": 1, "name": "Test Airport"}
        response = AirportService.get_airport(1)
        self.assertEqual(response, {"id": 1, "name": "Test Airport", "city_id": 1})

    @patch("flight_planner.services.storage")
    def test_g_get_airport_not_found(self, mock_storage):
        mock_storage.get_airport.return_value = None
        with self.assertRaises(KeyError) as context:
            AirportService.get_airport(1)
        tear_down_module()

    @patch("flight_planner.services.storage")
    def test_e_delete_airport(self, mock_storage):
        response = AirportService.delete_airport(1)
        self.assertEqual(response, "")

    @patch("flight_planner.services.storage")
    def test_f_delete_all_airports(self, mock_storage):
        response = AirportService.delete_all_airports()
        self.assertEqual(response, "")


class TestFlightService(unittest.TestCase):
    @patch("flight_planner.services.storage")
    def test_a_create_flight(self, mock_storage):
        mock_storage.create_flight.return_value = {"id": 1, "name": "Test Flight"}
        response = FlightService.create_flight({"name": "Test Flight"})
        self.assertEqual(response, {"id": 1, "name": "Test Flight"})

    @patch("flight_planner.services.storage")
    def test_b_get_all_flights(self, mock_storage):
        mock_storage.get_all_flights.return_value = [{"id": 1, "name": "Test Flight"}]
        response = FlightService.get_all_flights()
        self.assertEqual(response, [{"id": 1, "name": "Test Flight"}])

    @patch("flight_planner.services.storage")
    def test_c_get_flight(self, mock_storage):
        mock_storage.get_flight.return_value = {"id": 1, "name": "Test Flight"}
        response = FlightService.get_flight(1)
        self.assertEqual(response, {"id": 1, "name": "Test Flight"})

    @patch("flight_planner.services.storage")
    def test_g_get_flight_not_found(self, mock_storage):
        mock_storage.get_flight.return_value = None
        with self.assertRaises(KeyError) as context:
            FlightService.get_flight(999)
        tear_down_module()

    @patch("flight_planner.services.storage")
    def test_e_delete_flight(self, mock_storage):
        response = FlightService.delete_flight(1)
        self.assertEqual(response, "")

    @patch("flight_planner.services.storage")
    def test_f_delete_all_flights(self, mock_storage):
        response = FlightService.delete_all_flights()
        self.assertEqual(response, "")


class TestCityServiceAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        app = create_app()
        app.config["TESTING"] = True
        cls.client = app.test_client()

    def test_1_get_all_cities(self):
        response = self.client.get("/cities/")
        self.assertEqual(response.status_code, 200)  # Check if status code is 200
        data = json.loads(response.data)
        self.assertIsInstance(data, list)  # Check if the response is a list

    def test_2_create_city(self):
        payload = {"name": "New York"}
        response = self.client.post(
            "/cities/", data=json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, 201)  # Check if status code is 201
        data = json.loads(response.data)
        self.assertIn("id", data)  # Check if 'id' is in the response
        self.assertEqual(data["name"], "New York")  # Check if the name is correct
        # self.client.delete('/cities/1')

    def test_3_create_city_invalid_data(self):
        # Test creating a city with invalid data
        payload = {}  # Missing 'name'
        with self.assertRaises(KeyError):
            response = self.client.post(
                "/cities/", data=json.dumps(payload), content_type="application/json"
            )

    def test_4_get_city_not_found(self):
        # Test getting a city that does not exist
        response = self.client.get(
            "/cities/9999/"
        )  # Assuming 9999 is a non-existent ID
        self.assertEqual(response.status_code, 404)

    def test_6_delete_city(self):
        """ "Test for deleting a city by ID"""
        # Test getting a city that does not exist
        response = self.client.get("/cities/")
        response = self.client.delete("/cities/1")
        self.assertEqual(response.status_code, 204)  # Check if status code is 204

    def test_7_delete_all_cities(self):
        response = self.client.delete("/cities/delete_all")
        response = self.client.get("/cities/")
        self.assertEqual(json.loads(response.data), [])
        tear_down_module()


class TestAirportServiceAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        app = create_app()
        app.config["TESTING"] = True
        cls.client = app.test_client()

    def test_1_get_all_airports(self):
        response = self.client.get("/airports/")
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIsInstance(data, list)

    def test_2_create_airport(self):
        """ "This test tries to create an airport"""
        payload2 = {"name": "Reikjavik"}
        self.client.post(
            "/cities/", data=json.dumps(payload2), content_type="application/json"
        )

        payload = {"name": "JFK Airport", "city_id": 1}
        response = self.client.post(
            "/airports/", data=json.dumps(payload), content_type="application/json"
        )

        self.assertEqual(response.status_code, 201)
        data = json.loads(response.data)
        self.assertIn("id", data)
        self.assertEqual(data["name"], "JFK Airport")

    def test_3_create_airport_invalid_data(self):
        payload = {}
        with self.assertRaises(KeyError):
            response = self.client.post(
                "/airports/", data=json.dumps(payload), content_type="application/json"
            )

    def test_4_get_airport_not_found(self):
        """ "This test tries to get airport by its id of unknown id"""

        response = self.client.get("/airports/9999/")
        self.assertEqual(response.status_code, 404)

    def test_5_get_airport_by_id(self):
        """ "This test tries to get airport by its id"""

        payload_city = {"name": "Lahti"}
        self.client.post(
            "/cities/", data=json.dumps(payload_city), content_type="application/json"
        )

        payload = {"name": "LAX Airport", "city_id": 1}
        response = self.client.post(
            "/airports/", data=json.dumps(payload), content_type="application/json"
        )

        response = self.client.get("/airports/2")  # Adjust the ID as needed
        data = json.loads(response.data)
        self.assertEqual(data["name"], "LAX Airport")

    def test_6_delete_airport(self):
        response = self.client.delete("/airports/1")  # Adjust the ID as needed
        response = self.client.get("/airports/1")
        self.assertEqual(response.status_code, 404)

    def test_7_delete_all_airports(self):

        response = self.client.delete("/airports/delete_all")
        response = self.client.get("/airports/")
        self.assertEqual(json.loads(response.data), [])
        tear_down_module()

    def test_8_update_airport(self):
        """ "This test the update method by id for airport"""
        tear_down_module()
        payload1 = {"name": "Boston"}
        self.client.post(
            "/cities/", data=json.dumps(payload1), content_type="application/json"
        )
        payload2 = {"name": "Miami"}
        self.client.post(
            "/cities/", data=json.dumps(payload2), content_type="application/json"
        )
        boston = CityService.get_city_from_name("Boston")
        miami = CityService.get_city_from_name("Miami")

        payload3 = {"name": "BOSTON Airport", "city_id": boston}
        self.client.post(
            "/airports/", data=json.dumps(payload3), content_type="application/json"
        )
        payload_new = {"name": "MIAMI Airport", "city_id": miami}
        self.client.put(
            "/airports/1", data=json.dumps(payload_new), content_type="application/json"
        )
        response = self.client.get("/airports/1")
        data = json.loads(response.data)
        self.assertEqual(data["name"], "MIAMI Airport")

    def test_9_update_all_airports(self):
        """ "This test the update method for all airports"""
        tear_down_module()
        # Setup for the test
        payload1 = {"name": "Boston"}
        self.client.post(
            "/cities/", data=json.dumps(payload1), content_type="application/json"
        )
        payload2 = {"name": "Miami"}
        self.client.post(
            "/cities/", data=json.dumps(payload2), content_type="application/json"
        )
        boston = CityService.get_city_from_name("Boston")
        miami = CityService.get_city_from_name("Miami")

        payload3 = {"name": "BOSTON Airport", "city_id": boston}
        self.client.post(
            "/airports/", data=json.dumps(payload3), content_type="application/json"
        )
        payload4 = {"name": "MIAMI Airport", "city_id": miami}
        self.client.post(
            "/airports/", data=json.dumps(payload4), content_type="application/json"
        )

        payload5 = {"name": "Triest"}
        self.client.post(
            "/cities/", data=json.dumps(payload5), content_type="application/json"
        )
        payload6 = {"name": "Sofia"}
        self.client.post(
            "/cities/", data=json.dumps(payload6), content_type="application/json"
        )
        triest = CityService.get_city_from_name("Triest")
        sofia = CityService.get_city_from_name("Sofia")

        payload_updated = {
            "1": {"name": "Triest Airport", "city_id": triest},
            "2": {"name": "SOFIA T2 Airport", "city_id": sofia},
        }
        self.client.put(
            "/airports/",
            data=json.dumps(payload_updated),
            content_type="application/json",
        )

        response = self.client.get("/airports/")
        data = json.loads(response.data)
        self.assertEqual(data[0]["name"], "Triest Airport")
        self.assertEqual(data[1]["name"], "SOFIA T2 Airport")
        tear_down_module()


class TestFlightServiceAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        app = create_app()
        app.config["TESTING"] = True
        cls.client = app.test_client()

    def test_1_get_all_flights(self):
        response = self.client.get("/flights/")
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIsInstance(data, list)

    def test_2_create_flight(self):
        payload = {
            "name": "Flight 101",
            "departureAirport": "New York",
            "arrivalAirport": "Los Angeles",
            "price": 300,
            "departureTime": "14:30",
            "arrivalTime": "17:30",
        }
        payload2 = {"name": "New York"}
        response = self.client.post(
            "/cities/", data=json.dumps(payload2), content_type="application/json"
        )
        payload2 = {"name": "Los Angeles"}
        response = self.client.post(
            "/cities/", data=json.dumps(payload2), content_type="application/json"
        )

        ny = CityService.get_city_from_name("New York")
        la = CityService.get_city_from_name("Los Angeles")

        payload3 = {"name": "New York", "city_id": ny}
        self.client.post(
            "/airports/", data=json.dumps(payload3), content_type="application/json"
        )
        payload4 = {"name": "Los Angeles", "city_id": la}
        self.client.post(
            "/airports/", data=json.dumps(payload4), content_type="application/json"
        )

        response = self.client.post(
            "/flights/", data=json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, 201)
        data = json.loads(response.data)
        self.assertIn("id", data)
        self.assertEqual(data["name"], "Flight 101")
        # self.client.delete('/cities/2')
        # self.client.delete('/cities/3')

    def test_3_create_flight_invalid_data(self):
        payload = {}  # Missing other required fields
        with self.assertRaises(KeyError):
            response = self.client.post(
                "/flights/", data=json.dumps(payload), content_type="application/json"
            )

    def test_4_get_flight_not_found(self):
        response = self.client.get("/flights/9999/")
        self.assertEqual(response.status_code, 404)

    def test_5_get_flight_by_id(self):
        payload = {
            "name": "Flight 202",
            "departure_city": "Boston",
            "arrival_city": "Miami",
            "price": 250,
            "departure_time": "10:00",
            "arrival_time": "13:00",
        }
        payload2 = {"name": "Boston"}
        response = self.client.post(
            "/cities/", data=json.dumps(payload2), content_type="application/json"
        )
        payload2 = {"name": "Miami"}
        response = self.client.post(
            "/cities/", data=json.dumps(payload2), content_type="application/json"
        )
        boston = CityService.get_city_from_name("Boston")
        miami = CityService.get_city_from_name("Miami")
        payload3 = {"name": "Boston", "city_id": boston}
        self.client.post(
            "/airports/", data=json.dumps(payload3), content_type="application/json"
        )
        payload4 = {"name": "Miami", "city_id": miami}
        self.client.post(
            "/airports/", data=json.dumps(payload4), content_type="application/json"
        )

        response = self.client.post(
            "/flights/", data=json.dumps(payload), content_type="application/json"
        )
        response = self.client.get("/flights/2")
        data = json.loads(response.data)
        self.assertEqual(data["name"], "Flight 202")
        self.assertEqual(data["price"], 250)
        self.assertEqual(data["departure_city"], 3)
        self.assertEqual(data["arrival_city"], 4)

        # self.client.delete('/cities/1')
        # self.client.delete('/cities/2')

    #
    def test_6_delete_flight(self):
        response = self.client.delete("/flights/1")  # Adjust the ID as needed
        self.assertEqual(response.status_code, 200)

    def test_7_delete_all_flights(self):
        response = self.client.delete("/flights/delete_all")
        response = self.client.get("/flights/")
        print(json.loads(response.data))
        self.assertEqual(json.loads(response.data), [])
        tear_down_module()

    def test_8_search_for_flights(self):
        """ "This test is for search of a flight by parameters"""
        tear_down_module()
        # Setup for the test
        payload1 = {"name": "Boston"}
        self.client.post(
            "/cities/", data=json.dumps(payload1), content_type="application/json"
        )
        payload2 = {"name": "Miami"}
        self.client.post(
            "/cities/", data=json.dumps(payload2), content_type="application/json"
        )
        boston = CityService.get_city_from_name("Boston")
        miami = CityService.get_city_from_name("Miami")

        payload3 = {"name": "BOSTON Airport", "city_id": boston}
        self.client.post(
            "/airports/", data=json.dumps(payload3), content_type="application/json"
        )
        payload4 = {"name": "MIAMI Airport", "city_id": miami}
        self.client.post(
            "/airports/", data=json.dumps(payload4), content_type="application/json"
        )
        ##
        payload_create_city = {
            "name": "Flight 101",
            "departureAirport": "BOSTON Airport",
            "arrivalAirport": "MIAMI Airport",
            "price": 300,
            "departureTime": "14:30",
            "arrivalTime": "17:30",
        }

        self.client.post(
            "/flights/",
            data=json.dumps(payload_create_city),
            content_type="application/json",
        )

        payload_search_city = {
            "name": "Flight 101",
            "departureAirport": "id1",
            "arrivalAirport": "id2",
            "min_price": 290,
            "max_price": 310,
            "min_departure_time": "13:30",
            "max_departure_time": "15:30",
            "min_arrival_time": "16:30",
            "max_arrival_time": "18:30",
        }
        response = self.client.post(
            "/flights/search",
            data=json.dumps(payload_search_city),
            content_type="application/json",
        )
        response2 = self.client.get("/flights/1")
        self.assertEqual(json.loads(response.data), [json.loads(response2.data)])

    def test_9_test_update_flights(self):
        """ "This test tries if the update flight by idmethod is working"""
        payload_create_city = {
            "name": "Flight 150",
            "departureAirport": "BOSTON Airport",
            "arrivalAirport": "MIAMI Airport",
            "price": 350,
            "departureTime": "15:00",
            "arrivalTime": "16:00",
        }
        response = self.client.put(
            "/flights/1",
            data=json.dumps(payload_create_city),
            content_type="application/json",
        )
        self.assertEqual(json.loads(response.data), "")
        tear_down_module()


class TestInitializationFromCSV(unittest.TestCase):
    def test_initialize(self):
        """ "This test tries to initialize the SQL from CSVs"""
        tear_down_module()
        cities = ObjectService.initialize_database("cities")
        airports = ObjectService.initialize_database("airports")
        flights = ObjectService.initialize_database("flights")
        self.assertEqual(cities, "")
        self.assertEqual(airports, "")
        self.assertEqual(flights, "")
        tear_down_module()


def tear_down_module():
    """Function to delete all data from all tables after all tests have run"""
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("DELETE FROM cities")
    cursor.execute("DELETE FROM airports")
    cursor.execute("DELETE FROM flights")
    connection.commit()
    connection.close()


if __name__ == "__main__":
    unittest.main()
