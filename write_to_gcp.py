import pandas as pd
from google.cloud import bigquery
from weather_api import get_weather_data

client = bigquery.Client()


def write_to_bigquery(data):

    table_id = "seu_projeto.sua_dataset.sua_tabela"

    df = pd.DataFrame(data, index=[0])

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print("Dados escritos no BigQuery com sucesso.")

if __name__ == "__main__":

    weather_data = get_weather_data()
    
    if weather_data:

        data_df = pd.read_csv("dados_climaticos.csv")

        combined_data = {
            "Timestamp": [weather_data["current"]["last_updated"]],
            "Temperature (°C)": [weather_data["current"]["temp_c"]],
            "Humidity (%)": [weather_data["current"]["humidity"]]
        }

        write_to_bigquery(combined_data)
