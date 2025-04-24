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
    
    # Criando diretório para dados temporários se não existir
    os.makedirs('temp', exist_ok=True)
    
    # Data e hora atual para o arquivo de saída
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"temp/weather_data_{timestamp}.json"
    
    weather_data = []
    
    # Percorre a lista de cidades para obter dados meteorológicos
    for city in CITIES:
        try:
            # Montando a URL da API com a cidade atual e a chave de API
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}&units=metric&lang=pt_br"
            
            # Fazendo a requisição à API
            response = requests.get(url)
            response.raise_for_status()  # Levanta exceção se houver erro HTTP
            
            # Parseando os dados JSON da resposta
            data = response.json()
            
            # Adicionando timestamp de coleta e cidade aos dados
            data['extraction_timestamp'] = datetime.now().isoformat()
            data['city_name'] = city
            
            # Adicionando os dados à lista
            weather_data.append(data)
            
            print(f"Dados extraídos com sucesso para: {city}")
            
        except Exception as e:
            print(f"Erro ao extrair dados para {city}: {str(e)}")
    
    # Salvando todos os dados em um arquivo JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=4)
    
    print(f"Extração concluída. Dados salvos em: {output_file}")
    
    # Retorna o caminho do arquivo para a próxima tarefa
    return output_file
