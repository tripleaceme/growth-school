# Import Dependencies
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator
import openmeteo_requests
import requests_cache
from retry_requests import retry
import time
import pandas as pd



# Define Default Arguments
default_args = {
    'owner': 'Growth School',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG Object
dag = DAG('data_orchestration',
    default_args=default_args,
    description='Growth RediStore Data Orchestration DAG',
    schedule_interval=timedelta(days=1),
)

# Define Global Variables
postgresql_engine = create_engine("postgresql://growth:growth-school@localhost:5432/GrediStore")

postgresql_conn = postgresql_engine.connect()


# Functions
def create_postgresql_tables():
    # Create tables in PostgreSQL (replace with actual table names and schema)
    
    postgresql_conn.execute('''
        DROP TABLE IF EXISTS growth_schema.customers CASCADE;
        CREATE TABLE IF NOT EXISTS growth_schema.customers (
            customer_id CHAR(5) PRIMARY KEY,
            first_name VARCHAR(255) NOT NULL,
            last_name VARCHAR(255) NOT NULL,
            date_of_birth DATE NOT NULL,
            email  VARCHAR(255) NOT NULL UNIQUE,
            gender VARCHAR(6) CHECK(gender IN ('Female', 'Male')),
            city VARCHAR(255) NOT NULL,
            age CHAR(2) NOT NULL,
            address VARCHAR(255) NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL
            
        )'''
    )

    # Create products table

    postgresql_conn.execute('''
        DROP TABLE IF EXISTS growth_schema.products CASCADE;
        CREATE TABLE IF NOT EXISTS growth_schema.products (
            product_id CHAR(5) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            unit_price DOUBLE PRECISION NOT NULL,
            category VARCHAR(255) NOT NULL
        )'''
    )


    # Create orders table

    postgresql_conn.execute('''
        CREATE TABLE IF NOT EXISTS growth_schema.orders (
            invoice_no CHAR(6) NOT NULL,
            quantity INT NOT NULL,
            customer_id CHAR(5) NOT NULL,
            product_id CHAR(5) NOT NULL,
            channel  VARCHAR(10) CHECK(channel IN ('Website', 'Warehouse')),
            invoice_date TIMESTAMP NOT NULL,
            FOREIGN KEY(customer_id) REFERENCES growth_schema.customers(customer_id) ON DELETE CASCADE,
            FOREIGN KEY(product_id) REFERENCES growth_schema.products(product_id) ON DELETE CASCADE
        )'''
    )


def insert_data_into_customer():
    
    
    cust_data = pd.read_csv('dataset/customers.csv', encoding='latin1')

    # Get unique values in the 'Cities' column for our API usage
    unique_values = cust_data['city'].unique()

    # Convert to a Pandas Series
    unique_series = pd.Series(unique_values)

    # Save to CSV
    unique_series.to_csv('dataset/uni_cities.csv', index=False, header=['city'])

    cust_data['date_of_birth'] = pd.to_datetime(cust_data['date_of_birth'], errors='coerce')

    cust_data['date_of_birth'] = cust_data['date_of_birth'].dt.date

    dob = pd.to_datetime(cust_data['date_of_birth'])

    # Calculate today's date
    today = datetime.today()

    # Calculate age in days
    age_date = ((today - dob).dt.days)

    # Convert age to years
    cust_data['age'] = (age_date // 365).astype(int)
    
    # Iterate through each row in the 'customers' DataFrame
    for index, row in cust_data.iterrows():
        # Execute an SQL INSERT statement to add a row to the 'customers' table in the 'growth_schema' schema
        postgresql_conn.execute('''
            INSERT INTO growth_schema.customers (customer_id, first_name, last_name, date_of_birth, email, gender, city, age) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ''', 
            (row['customer_id'], row['first_name'], row['last_name'], row['date_of_birth'], 
                                    row['email'], row['gender'],row['city'],row['age']))


def insert_data_into_products():

    products = pd.read_csv('dataset/products.csv')

    # Iterate through each row in the 'products' DataFrame
    for index, row in products.iterrows():
        # Execute an SQL INSERT statement to add a row to the 'products' table in the 'growth_schema' schema
        postgresql_conn.execute('''
            INSERT INTO growth_schema.products (ProductID, name, UnitPrice) 
            VALUES (%s, %s, %s)''', 
            (row['ProductID'], row['name'], row['UnitPrice']))


def insert_data_into_orders():
    
    orders_df = pd.read_csv('dataset/orders.csv')
    
    # Iterate through each row in the 'orders' DataFrame
    for index, row in orders_df.iterrows():
        # Execute an SQL INSERT statement to add a row to the 'orders' table in the 'growth_schema' schema
        postgresql_conn.execute('''
            INSERT INTO growth_schema.orders (InvoiceNo, Quantity, CustomerID, ProductID, Channel, InvoiceDate) 
            VALUES (%s, %s, %s, %s, %s, %s)''', 
            (row['InvoiceNo'], row['Quantity'], row['CustomerID'], row['ProductID'], row['Channel'], row['InvoiceDate']))


def get_weather_data():
     
    # Load customer data
    customer_df = pd.read_csv('dataset/customers.csv')

    input_data = customer_df[['city', 'latitude', 'longitude']]

    unique_cities = input_data.drop_duplicates()

    # Initialize an empty DataFrame to store the combined data
    final_dataframe = pd.DataFrame()

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    for index, row in unique_cities.iterrows():
        latitude = row['latitude']
        longitude = row['longitude']
        
        # Check if latitude and longitude are within valid ranges
        if -90 <= latitude <= 90 and -180 <= longitude <= 180:
        
            # Set latitude and longitude in params
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "start_date": "2010-01-01",
                "end_date": "2011-10-31",
                "hourly": [
                    "temperature_2m", "relative_humidity_2m", "dew_point_2m",
                    "apparent_temperature", "precipitation", "rain", "snowfall",
                    "snow_depth", "weather_code", "pressure_msl", "surface_pressure",
                    "cloud_cover", "wind_speed_10m"
                ]
            }

            # The order of variables in hourly or daily is important to assign them correctly below
            url = "https://archive-api.open-meteo.com/v1/archive"
            
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]

            # Process hourly data. The order of variables needs to be the same as requested.
            hourly = response.Hourly()
            hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
            hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
            hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
            hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
            hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
            hourly_rain = hourly.Variables(5).ValuesAsNumpy()
            hourly_snowfall = hourly.Variables(6).ValuesAsNumpy()
            hourly_snow_depth = hourly.Variables(7).ValuesAsNumpy()
            hourly_weather_code = hourly.Variables(8).ValuesAsNumpy()
            hourly_pressure_msl = hourly.Variables(9).ValuesAsNumpy()
            hourly_surface_pressure = hourly.Variables(10).ValuesAsNumpy()
            hourly_cloud_cover = hourly.Variables(11).ValuesAsNumpy()
            hourly_wind_speed_10m = hourly.Variables(12).ValuesAsNumpy()

            hourly_data = {"date": pd.date_range(
                start = pd.to_datetime(hourly.Time(), unit = "s"),
                end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
                freq = pd.Timedelta(seconds = hourly.Interval()),
                inclusive = "left"
            )}
            hourly_data["temp"] = hourly_temperature_2m
            hourly_data["rel_hum"] = hourly_relative_humidity_2m
            hourly_data["dew_pnt"] = hourly_dew_point_2m
            hourly_data["apparent_temp"] = hourly_apparent_temperature
            hourly_data["precip"] = hourly_precipitation
            hourly_data["rain"] = hourly_rain
            hourly_data["snowfall"] = hourly_snowfall
            hourly_data["snow_depth"] = hourly_snow_depth
            hourly_data["weather_code"] = hourly_weather_code
            hourly_data["pressure_msl"] = hourly_pressure_msl
            hourly_data["surface_pressure"] = hourly_surface_pressure
            hourly_data["cloud_cover"] = hourly_cloud_cover
            hourly_data["wind_speed"] = hourly_wind_speed_10m
            hourly_data["latitude"] = latitude
            hourly_data["longitde"] = longitude

            hourly_dataframe = pd.DataFrame(data = hourly_data)

            # Append the current location's data to the final DataFrame
            final_dataframe = pd.concat([final_dataframe, hourly_dataframe], ignore_index=True)

            # Add a 1-minute wait before making the next request
            time.sleep(60)

        else:
            pass
            #print(f"Skipping invalid coordinates: Latitude={latitude}, Longitude={longitude}")
    final_dataframe.to_csv('dataset/weathers.csv')


def create_and_populate_weather_data():
        

    w_data = pd.read_csv('dataset/weathers.csv')

    # create table for weather
    postgresql_conn.execute('''
                            
        DROP TABLE IF EXISTS growth_schema.weather CASCADE;
        CREATE TABLE IF NOT EXISTS growth_schema.weather (
                            date TIMESTAMP NOT NULL,
                            temp DOUBLE PRECISION NOT NULL, 
                            rel_hum DOUBLE PRECISION NOT NULL, 
                            dew_pnt DOUBLE PRECISION NOT NULL, 
                            apparent_temp  DOUBLE PRECISION NOT NULL,
                            precip  DOUBLE PRECISION NOT NULL, 
                            rain DOUBLE PRECISION NOT NULL, 
                            snowfall DOUBLE PRECISION NOT NULL,
                            snow_depth DOUBLE PRECISION NOT NULL, 
                            weather_code DOUBLE PRECISION NOT NULL,
                            pressure_msl DOUBLE PRECISION NOT NULL, 
                            surface_pressure DOUBLE PRECISION NOT NULL, 
                            cloud_cover DOUBLE PRECISION NOT NULL, 
                            wind_speed DOUBLE PRECISION NOT NULL, 
                            latitude DOUBLE PRECISION NOT NULL, 
                            longitude DOUBLE PRECISION NOT NULL
    )'''
)

    for index, row in w_data.iterrows():
        postgresql_conn.execute('''
            INSERT INTO growth_schema.weather (date, temp, rel_hum, dew_pnt, apparent_temp, precip, rain,
                                                snowfall, snow_depth, weather_code, pressure_msl,
                                                surface_pressure, cloud_cover, wind_speed,latitude,longitude ) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', 
            
            (row['date'], row['temp'], row['rel_hum'], row['dew_pnt'], row['apparent_temp'], row['precip'], row['rain'],
                                        row['snowfall'], row['snow_depth'], row['weather_code'], row['pressure_msl'],
                                        row['surface_pressure'], row['cloud_cover'], row['wind_speed'], row['latitude'], row['longitude']))


    # Close connection
    postgresql_conn.close()


"""
def facts_tb():
    print('facts')


def dims_tb():
    print('dims')
"""


# Define tasks to use the functions

start_task = DummyOperator(task_id='start_task', dag=dag)


create_tables_task = PythonOperator(
    task_id='create_tables_task',
    python_callable=create_postgresql_tables,
    dag=dag,
)


insert_data_into_customer_task = PythonOperator(
    task_id='insert_data_into_customer_task',
    python_callable=insert_data_into_customer,
    dag=dag,
)


insert_data_into_products_task = PythonOperator(
    task_id='insert_data_into_products_task',
    python_callable=insert_data_into_products,
    dag=dag,
)


insert_data_into_orders_task = PythonOperator(
    task_id='insert_data_into_orders_task',
    python_callable=insert_data_into_orders,
    dag=dag,
)


get_weather_data_task = PythonOperator(
    task_id='get_weather_data_task',
    python_callable=get_weather_data,
    dag=dag,
)


create_and_populate_weather_data_task = PythonOperator(
    task_id='create_and_populate_weather_data_task',
    python_callable=create_and_populate_weather_data,
    dag=dag,
)


""" 
facts_tb_task = PythonOperator(
    task_id='facts_tb_task',
    python_callable=facts_tb,
    dag=dag,
)


dims_tb_task = PythonOperator(
    task_id='dims_tb_task',
    python_callable=dims_tb,
    dag=dag,
)
"""


end_task = DummyOperator(task_id='end_task', dag=dag)


# Set up task dependencies
start_task >> create_tables_task
start_task >> get_weather_data_task

create_tables_task >> insert_data_into_customer_task
create_tables_task >> insert_data_into_products_task
create_tables_task >> insert_data_into_orders_task

[insert_data_into_customer_task, insert_data_into_products_task, insert_data_into_orders_task] >> end_task

get_weather_data_task >> create_and_populate_weather_data_task  >> end_task


#create_and_populate_weather_data_task >> [facts_tb_task, dims_tb_task] >> end_task





