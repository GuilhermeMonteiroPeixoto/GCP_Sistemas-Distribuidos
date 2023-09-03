import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from weather_api import get_weather_data

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "YOUR_CREDENTIALS_FILE.json"

client = bigquery.Client()

def bq_create_dataset(client, dataset_name):
    dataset_ref = client.dataset(dataset_name)

    try:
        dataset = client.get_dataset(dataset_ref)
        print('Dataset {} already exists.'.format(dataset_name))
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        dataset = client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset_name))
    return dataset

def bq_create_table(client, dataset_name, table_name):
    dataset_ref = client.dataset(dataset_name)

    table_ref = dataset_ref.table(table_name)

    try:
        table = client.get_table(table_ref)
        print('Table {} already exists.'.format(table_name))
    except NotFound:
        schema = [
            bigquery.SchemaField('Timestamp', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('Temperature (°C)', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('Humidity (%)', 'INTEGER', mode='REQUIRED'),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print('Table {} created.'.format(table_name))
    return table

def export_items_to_bigquery(client, dataset_name, table_name, data):

    dataset_ref = client.dataset(dataset_name)

    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)

    errors = client.insert_rows(table, data)
    assert errors == []

if __name__ == "__main__":

    weather_data = get_weather_data()
    
    if weather_data:

        data_df = pd.read_csv("dados_climaticos.csv")
        
        data_to_insert = data_df.to_records(index=False)

        bq_create_dataset(client, "YOUR_DATASET_NAME")
        bq_create_table(client, "YOUR_DATASET_NAME", "YOUR_TABLE_NAME")
        export_items_to_bigquery(client, "YOUR_DATASET_NAME", "YOUR_TABLE_NAME", data_to_insert)
