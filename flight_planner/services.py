from datetime import datetime, timedelta
import re
import storage
from object_service_class import ObjectService
from database import get_db_connection
from typing import Any, Dict, List, Optional
from flight_planner.api_object import ApiObject

class CityService(ObjectService):
    """Service to manage city objects with static methods"""

    @staticmethod
    def get_city_from_name(name: str) -> int:
        connection = get_db_connection()
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT id FROM cities WHERE name = ?", (name,))
            result = cursor.fetchone()
            if result is None:
                CityService.create_city({"name": name})
                to_return = CityService.get_city_from_name(name)
                return to_return['id']

            return result["id"]
        finally:
            connection.close()

    @staticmethod
    def create_city(city_data: Dict[str, Any]) -> ApiObject:
        city_to_create=ApiObject()
        city_to_create.set_all_to_none()
        city_to_create.create_from_dict(city_data)
        return ObjectService.create_object(city_to_create, "cities")

    @staticmethod
    def get_all_cities() -> List[ApiObject]:
        return ObjectService.get_all_objects("cities")

    @staticmethod
    def get_city(city_id: int) -> ApiObject:
        return ObjectService.get_object(city_id, "cities")

    @staticmethod
    def delete_city(city_id: int) -> str:
        return ObjectService.delete_object(city_id, "cities")

    @staticmethod
    def delete_all_cities() -> str:
        ObjectService.delete_all_objects("cities")
        return ""

class AirportService(ObjectService):
    """Service to manage airport objects with static methods"""

    @staticmethod
    def get_airport_from_name(name: str) -> int:
        connection = get_db_connection()
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT id FROM airports WHERE name = ?", (name,))
            result = cursor.fetchone()
            if result is None:
                AirportService.create_airport({"name": name})
                to_return = AirportService.get_airport_from_name(name)
                return to_return['id']

            return result["id"]
        finally:
            connection.close()

    @staticmethod
    def create_airport(airport_data: Dict[str, Any]) -> ApiObject:
        if "city_id" in airport_data:
            try:
                CityService.get_city(airport_data["city_id"])
            except KeyError:
                raise KeyError("There is no city with such an ID")

        airport_object=ApiObject()
        airport_object.set_all_to_none()
        airport_object.create_from_dict(airport_data)
        return ObjectService.create_object(airport_object, "airports")

    @staticmethod
    def get_all_airports() -> List[ApiObject]:
        return ObjectService.get_all_objects("airports")

    @staticmethod
    def update_all_airports(update_data: Dict[str, Any]) -> str:
        list_of_airports=[]
        for i in update_data:
            object=ApiObject()
            object.set_all_to_none()
            object.create_from_dict(update_data[i])
            list_of_airports.append(object)

        return ObjectService.update_all_objects(list_of_airports, "airports")

    @staticmethod
    def update_airport(airport_id: int, update_data: Dict[str, Any]) -> str:
        object = ApiObject()
        object.set_all_to_none()
        object.create_from_dict(update_data)
        return ObjectService.update_object(airport_id, object, "airports")

    @staticmethod
    def get_airport(airport_id: int) -> ApiObject:
        return ObjectService.get_object(airport_id, "airports")

    @staticmethod
    def delete_airport(airport_id: int) -> str:
        return ObjectService.delete_object(airport_id, "airports")

    @staticmethod
    def delete_all_airports() -> str:
        ObjectService.delete_all_objects("airports")
        return ""


class FlightService(ObjectService):
    """A bunch of @staticmethod's"""

    @staticmethod
    def to_number(value: Any) -> float:
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                raise ValueError(f"Cannot convert {value} to either int or float")

    @staticmethod
    def to_snake_case(string: str) -> str:
        return re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()

    @staticmethod
    def search_flights(search_params: Dict[str, Any]) -> List[ApiObject]:
        params = []
        query = "SELECT * FROM flights WHERE 1=1"

        if "name" in search_params:
            query += " AND name = ?"
            params.append(search_params["name"])

        if "departure_city" in search_params:
            departure_city = FlightService.to_number(
                AirportService.get_airport_from_name(search_params["departure_city"])
            )
            query += " AND departure_city = ?"
            params.append(departure_city)

        if "arrival_city" in search_params:
            arrival_city = FlightService.to_number(
                AirportService.get_airport_from_name(search_params["arrival_city"])
            )
            query += " AND arrival_city = ?"
            params.append(arrival_city)

        if "min_price" in search_params:
            query += " AND price >= ?"
            params.append(FlightService.to_number(search_params["min_price"]))

        if "max_price" in search_params:
            query += " AND price <= ?"
            params.append(FlightService.to_number(search_params["max_price"]))

        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        connection.close()

        list_to_return=[]
        for row in rows:
            object=ApiObject()
            object.set_all_to_none()
            object.create_from_dict({key: value for key, value in dict(row).items() if value is not None})
            list_to_return.append(object)

        return list_to_return

    @staticmethod
    def calculate_arrival_time(departure_time: datetime, travel_time: int) -> str:
        arrival_dt = departure_time + timedelta(minutes=travel_time)
        return arrival_dt.strftime("%H:%M")

    @staticmethod
    def create_flight(dict:dict) -> ApiObject:
        dict_to_return = {}
        name = (
            departure_airport
        ) = arrival_airport = price = departure_time = arrival_time = travel_time = None
        for key, value in dict.items():
            if FlightService.to_snake_case(key) == "name":
                name = str(value)
                dict_to_return["name"] = name
            elif (
                FlightService.to_snake_case(key) == "departure_city"
                or FlightService.to_snake_case(key) == "departure_airport"
            ):
                departure_airport = FlightService.to_number(
                    AirportService.get_airport_from_name(value)
                )
                dict_to_return["departure_city"] = departure_airport
            elif (
                FlightService.to_snake_case(key) == "arrival_city"
                or FlightService.to_snake_case(key) == "arrival_airport"
            ):
                arrival_airport = FlightService.to_number(
                    AirportService.get_airport_from_name(value)
                )
                dict_to_return["arrival_city"] = arrival_airport
            elif FlightService.to_snake_case(key) == "price":
                price = FlightService.to_number(value)
                dict_to_return["price"] = price
            elif FlightService.to_snake_case(key) == "departure_time":
                departure_time = datetime.strptime(value, "%H:%M")
                dict_to_return["departure_time"] = departure_time
            elif FlightService.to_snake_case(key) == "travel_time":
                travel_time = FlightService.to_number(value)
                dict_to_return["travel_time"] = travel_time
            elif FlightService.to_snake_case(key) == "arrival_time":
                arrival_time = datetime.strptime(value, "%H:%M")
                dict_to_return["arrival_time"] = arrival_time

        try:
            dict_to_return["name"]
        except:
            raise (KeyError("Name is required argument for creating a flight"))

        latest_id = ObjectService.get_latest_id("flights")
        latest_id += 1
        dict_to_return["id"] = latest_id

        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute(
            f"""
            INSERT INTO flights (id,name, departure_city, arrival_city, price, departure_time, arrival_time,travel_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                latest_id,
                name,
                departure_airport,
                arrival_airport,
                price,
                departure_time,
                arrival_time,
                travel_time,
            ),
        )
        connection.commit()
        connection.close()

        object_to_return=ApiObject()
        object_to_return.set_all_to_none()
        object_to_return.create_from_dict(dict_to_return)
        return object_to_return

    @staticmethod
    def to_camel_case(snake_str):
        """Helper function to convert snake_case to camelCase"""
        components = snake_str.split("_")
        return components[0] + "".join(x.title() for x in components[1:])

    @staticmethod
    def get_all_flights(offset=0, max_count=50, sort_by="departure_time", sort_order="ASC") ->ApiObject:
        sort_order = (
            "ASC" if sort_order.upper() not in ["ASC", "DESC"] else sort_order.upper()
        )
        valid_sort_columns = [
            "id",
            "name",
            "departure_city",
            "arrival_city",
            "price",
            "departure_time",
            "arrival_time",
            "travel_time",
        ]
        sort_by = sort_by if sort_by in valid_sort_columns else "departure_time"

        query = f"""
            SELECT * FROM flights
            ORDER BY {sort_by} {sort_order}
            LIMIT ? OFFSET ?
        """

        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute(query, (max_count, offset))
        rows = cursor.fetchall()
        connection.close()



        flights_to_return=[]
        for row in rows:
            object=ApiObject()
            object.set_all_to_none()
            object.create_from_dict({key: value for key, value in dict(row).items() if value is not None})
            flights_to_return.append(object)

        return flights_to_return

    @staticmethod
    def delete_all_flights() -> str:
        ObjectService.delete_all_objects("flights")
        return ""

    @staticmethod
    def get_flight(flight_id: int) -> ApiObject:
        return ObjectService.get_object(flight_id, "flights")

    @staticmethod
    def delete_flight(id: int) -> str:
        return ObjectService.delete_object(id, "flights")

    @staticmethod
    def update_flight(id: int, dict: dict) -> str:
        try:
            obj = ObjectService.get_object(id, "flights").generate_dict()

        except:
            raise KeyError(f"Object with ID {id} is not in the current table!")
        ObjectService.delete_object(id, "flights")
        dict["id"] = obj["id"]
        FlightService.create_flight(dict)
        return ""
