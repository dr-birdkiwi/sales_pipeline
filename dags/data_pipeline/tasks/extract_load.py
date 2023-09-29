import csv
from datetime import datetime
import json

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from data_pipeline.helper.constants import POSTGRES_CONNECTION_ID, STORES
from data_pipeline.helper.generic import get_store_city_country


def add_store_country_city(source_file, sink_file):
    """Assigns a store to each order"""
    with open(source_file, 'r') as f_in, open(sink_file, 'w') as f_out:
        reader = csv.reader(f_in)
        next(reader)
        writer = csv.writer(f_out, delimiter='\t')
        writer.writerows(
            row + list(get_store_city_country(int(row[0]))) for row in reader)


def truncate_load(target_table, sink_file):
    """Truncate target table and load a file to it"""

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f'truncate table {target_table};')
            conn.commit()

    hook.bulk_load(target_table, sink_file)


def insert_only(target_table, sink_file):
    """Insert a file to a table"""

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
    hook.bulk_load(target_table, sink_file)


def retrieve_users(sink_file):
    """Get users via API"""

    res = requests.get('https://jsonplaceholder.typicode.com/users')
    columns = ['id', 'name', 'username', 'email',
               'address', 'phone', 'website', 'company']

    with open(sink_file, 'w') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=columns,
                                delimiter='\t', quotechar="'")
        for row in res.json():
            _row = {col: json.dumps(row[col]) if type(
                row[col]) is dict else row[col] for col in columns}
            writer.writerow(_row)


def retrieve_weather(sink_file):
    """Get today's weather for all the stores"""

    created_at = datetime.today()
    columns = ['created_at', 'country', 'city',
               'weather', 'main', 'wind', 'clouds', 'sys']

    with open(sink_file, 'w') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=columns,
                                delimiter='\t', quotechar="'")
        for item in STORES:
            city, country = item[0], item[1]
            url = f'https://api.openweathermap.org/data/2.5/weather?q={city}, {country}&APPID=125fac9db5416a47bebdc80e74518e9b&units=metric'
            res = requests.get(url)
            if res.status_code == 200:
                item = res.json()
                item['created_at'] = created_at
                item['country'] = country
                item['city'] = city
                row = {col: json.dumps(item[col]) if type(item[col]) in [
                    dict, list] else item[col] for col in columns}
                writer.writerow(row)
