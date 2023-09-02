import requests
import pandas as pd
import time
from datetime import datetime
from config import city, country, api_key

num_readings = 10
interval_minutes = 60

data_df = pd.DataFrame(columns=['Timestamp', 'Temperature (°C)', 'Humidity (%)'])

for i in range(num_readings):
    try:

        url = f'http://api.openweathermap.org/data/2.5/weather?q={city},{country}&appid={api_key}'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            temperatura_kelvin = data['main']['temp']
            temperatura_celsius = temperatura_kelvin - 273.15
            umidade = data['main']['humidity']
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data_df.loc[i] = [timestamp, temperatura_celsius, umidade]
            if i < num_readings - 1:
                time.sleep(interval_minutes * 60)
        else:
            print(f'Erro na solicitação: Código {response.status_code}')
            
    except Exception as e:
        print(f"Ocorreu um erro: {str(e)}")
data_df.to_csv('dados_climaticos.csv', index=False)
