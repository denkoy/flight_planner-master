import sqlite3
import datetime
def get_db_connection():
    sqlite3.register_adapter(datetime.datetime, lambda dt: dt.strftime("%Y-%m-%d %H:%M:%S"))
    sqlite3.register_converter("DATETIME", lambda s: datetime.datetime.strptime(s.decode(), "%Y-%m-%d %H:%M:%S"))
    connection = sqlite3.connect('database.db', detect_types=sqlite3.PARSE_DECLTYPES)
    connection.row_factory = sqlite3.Row
    return connection

def init_db():
    connection = get_db_connection()
    cursor = connection.cursor()
    # Create tables if they don't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS airports (
            id INTEGER PRIMARY KEY UNIQUE,
            name TEXT NOT NULL UNIQUE,
            city_id INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS flights (
            id INTEGER PRIMARY KEY UNIQUE,
            name TEXT NOT NULL UNIQUE,
            departure_city INTEGER,
            arrival_city INTEGER,
            price REAL,
            departure_time DATETIME,
            arrival_time DATETIME,
            travel_time INTEGER
        )
    ''')
    connection.commit()
    connection.close()

# Initialize the database
init_db()
