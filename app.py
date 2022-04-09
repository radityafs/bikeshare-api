from ast import Try
from flask import Flask, request
import pandas as pd
import sqlite3

app = Flask(__name__) 

@app.route('/stations')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/trips')
def get_all_trips_data():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/station/<station_id>')
def get_station_data(station_id):
    conn = make_connection()
    station_data = get_station_id(station_id, conn)
    return station_data.to_json()

@app.route('/trips/<trip_id>')
def get_trip_data(trip_id):
    conn = make_connection()
    trip_data = get_trip_id(trip_id, conn)
    return trip_data.to_json()


@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    ##tuple to string
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/add', methods=['POST'])
def route_add_trip():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/trips/average_duration') 
def get_average_duration():
    conn = make_connection()
    result = get_average_duration(conn)
    return str(result)

@app.route('/trips/average_duration/<bike_id>')
def get_average_duration_by_bike_id(bike_id):
    conn = make_connection()
    result = get_average_duration_by_bike_id(bike_id, conn)
    return str(result)

@app.route('/trips/summary_of_day', methods=['POST'])
def get_trip_time_of_day():
    # parse and transform incoming data into a tuple as we need 
    input_data = request.get_json() # Get the input as dictionary
    specified_date = input_data['period'] # Select specific items (period) from the dictionary (the value will be "2015-08")
    
    conn = make_connection()
    trip_of_day = get_trip_of_day(specified_date, conn)
    result = trip_of_day.groupby('start_station_id').agg({
    'bikeid' : 'count', 
    'duration_minutes' : 'mean'
    })

    # Return the result
    return result.to_json()


# Define a function to create connection for reusability purpose
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE trip_id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_all_trips(conn):
    query = f"""SELECT * FROM trips LIMIT 100"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_average_duration(conn):
    query = f"""SELECT AVG(duration_minutes) AS AvgDuration FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_of_day(specified_date, conn):
    query = f"""SELECT * FROM trips WHERE start_date LIKE '{specified_date}%'"""
    result = pd.read_sql_query(query, conn)
    return result


if __name__ == '__main__':
    app.run(debug=True, port=5000)