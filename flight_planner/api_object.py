"""
            "id",
            "name",
            "departure_city",
            "arrival_city",
            "price",
            "departure_time",
            "arrival_time",
            "travel_time"
"""
from datetime import datetime

class ApiObject:
    def __init__(self,id=None,name=None,departure_city=None,arrival_city=None,price=None,departure_time=None,arrival_time=None,travel_time=None,city_id=None):

        if id is not None and self.validate_id(id):
            self._id=id
        if city_id is not None and self.validate_id(city_id):
            self._city_id = city_id
        if name is not None:
            self._name=name
        if departure_city is not None and self.validate_departure_city():
            self._departure_city=departure_city
        if arrival_city is not None and self.validate_arrival_city():
            self._arrival_city=arrival_city
        if price is not None and self.validate_price():
            self._price=price
        if departure_time is not None and self.validate_departure_time():
            self._departure_time=departure_time
        if arrival_time is not None and self.validate_arrival_time():
            self._arrival_time=arrival_time
        if travel_time is not None and self.validate_travel_time():
            self._travel_time=travel_time

    def set_all_to_none(self):
            self._id = None
            self._name = None
            self._departure_city = None
            self._arrival_city = None
            self._price = None
            self._departure_time = None
            self._arrival_time = None
            self._travel_time = None
            self._city_id=None
    def create_from_dict(self,dictionary: dict):
        id = dictionary.get("id")
        name = dictionary.get("name")
        departure_city = dictionary.get("departure_city")
        arrival_city = dictionary.get("arrival_city")
        price = dictionary.get("price")
        departure_time = dictionary.get("departure_time")
        arrival_time = dictionary.get("arrival_time")
        travel_time = dictionary.get("travel_time")
        city_id = dictionary.get('city_id')

        if id is not None and self.validate_id(id):
            self._id = id
        if city_id is not None and self.validate_id(city_id):
            self._city_id = city_id
        if name is not None:
            self._name = name
        if departure_city is not None and self.validate_departure_city(departure_city):
            self._departure_city = departure_city
        if arrival_city is not None and self.validate_arrival_city(arrival_city):
            self._arrival_city = arrival_city
        if price is not None and self.validate_price(price):
            self._price = price
        if departure_time is not None and self.validate_departure_time(departure_time):
            self._departure_time = departure_time
        if arrival_time is not None and self.validate_arrival_time(arrival_time):
            self._arrival_time = arrival_time
        if travel_time is not None and self.validate_travel_time(travel_time):
            self._travel_time = travel_time



    def generate_dict(self) -> dict:
        dict_to_return={}
        if self._id is not None:
            dict_to_return['id'] = self._id
        if self._city_id is not None:
            dict_to_return['city_id'] = self._city_id
        if self._name is not None:
            dict_to_return['name']=self._name
        if self._departure_city is not None:
            dict_to_return['departure_city']=self._departure_city
        if self._arrival_city is not None:
            dict_to_return['arrival_city']=self._arrival_city
        if self._price is not None:
            dict_to_return['price']=self._price
        if self._departure_time is not None:
            dict_to_return['departure_time']=self._departure_time
        if self._arrival_time is not None:
            dict_to_return['arrival_time']=self._arrival_time
        if self._travel_time is not None:
            dict_to_return['travel_time']=self._travel_time
        return dict_to_return

    def validate_id(self,id:str):
        try:
            int(id)
        except:
            raise(TypeError("ID is not an integer."))
        return True
    def validate_departure_city(self, departure_city:str or int):
        if departure_city is not None:
            if not isinstance(departure_city,int) and not isinstance(departure_city,str) and (isinstance(departure_city,str) and departure_city.isalpha()):
                raise (TypeError("Departure city cannot contains symbols different from letters."))
        return True
    def validate_arrival_city(self, arrival_city:str or int):
        if arrival_city is not None:
            if not isinstance(arrival_city,int) and not isinstance(arrival_city,str) and (isinstance(arrival_city,str) and arrival_city.isalpha()):
                raise (TypeError("Arrival city cannot contains symbols different from letters."))
        return True

    def validate_price(self, price: str):
        try:
            float(price)
            int(price)
        except:
            raise (TypeError("Price is not a number."))
        return True

    def validate_travel_time(self, travel_time: str):
        try:
            float(travel_time)
            int(travel_time)
        except:
            raise (TypeError("Travel time is not a number."))
        return True

    def validate_departure_time(self,value):
        if not isinstance(value,datetime):
            raise (TypeError("Invalid departure time."))
        return True

    def validate_arrival_time(self,value):
        return self.validate_departure_time(value)
