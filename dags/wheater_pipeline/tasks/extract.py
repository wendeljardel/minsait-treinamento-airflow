import requests
import json
import os
from datetime import datetime
from airflow.decorators import task
from wheater_pipeline.config.config import OPENWEATHER_API_KEY, CITIES

@task(multiple_outputs=False)
def extract_weather_data():
    """
    Extrai dados meteorológicos da API OpenWeatherMap para uma lista de cidades
    e salva em um arquivo JSON para processamento posterior.
    
    Returns:
        str: O caminho para o arquivo de saída contendo os dados extraídos
    """
    print("Iniciando extração de dados meteorológicos...")
    
    
    os.makedirs('temp', exist_ok=True)
    
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"temp/weather_data_{timestamp}.json"
    
    weather_data = []
    
   
    for city in CITIES:
        try:
            # Montando a URL da API com a cidade atual e a chave de API
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}&units=metric&lang=pt_br"
            
            
            response = requests.get(url)
            response.raise_for_status()  
            
           
            data = response.json()
            
            
            data['extraction_timestamp'] = datetime.now().isoformat()
            data['city_name'] = city
            
            
            weather_data.append(data)
            
            print(f"Dados extraídos com sucesso para: {city}")
            
        except Exception as e:
            print(f"Erro ao extrair dados para {city}: {str(e)}")
    
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=4)
    
    print(f"Extração concluída. Dados salvos em: {output_file}")
    
    # Retorna o caminho do arquivo para a próxima tarefa
    return output_file
