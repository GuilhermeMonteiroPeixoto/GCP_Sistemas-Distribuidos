import requests
from config import api_key_wether_api

def get_weather_data():
    api_url = f'https://api.weatherapi.com/v1/current.json?key={api_key_wether_api}&q=New York'
    
    try:
        # Faz a solicitação à API
        response = requests.get(api_url)
        
        # Verifica se a solicitação foi bem-sucedida
        if response.status_code == 200:
            # Converte os dados da resposta para JSON
            data = response.json()
            return data
        else:
            print(f'Erro na solicitação: Código {response.status_code}')
            return None
            
    except Exception as e:
        print(f"Ocorreu um erro: {str(e)}")
        return None