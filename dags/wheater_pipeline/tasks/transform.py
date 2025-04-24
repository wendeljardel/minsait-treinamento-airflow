import json
import pandas as pd
import os
from datetime import datetime
from airflow.decorators import task

@task
def transform_weather_data(input_file: str):
    """
    Transforma os dados meteorológicos extraídos, limpando e 
    formatando para análise e armazenamento eficiente.
    
    Args:
        input_file (str): Caminho para o arquivo com os dados extraídos
        
    Returns:
        str: Caminho para o arquivo com os dados transformados
    """
    print("Iniciando transformação dos dados meteorológicos...")
    
    
    with open(input_file, 'r', encoding='utf-8') as f:
        weather_data = json.load(f)
    
    
    transformed_data = []
    
    for data in weather_data:
        try:
            
            transformed_item = {
                'city_id': data.get('id'),
                'city_name': data.get('city_name'),
                'country': data.get('sys', {}).get('country'),
                'latitude': data.get('coord', {}).get('lat'),
                'longitude': data.get('coord', {}).get('lon'),
                'temperature': data.get('main', {}).get('temp'),
                'feels_like': data.get('main', {}).get('feels_like'),
                'temp_min': data.get('main', {}).get('temp_min'),
                'temp_max': data.get('main', {}).get('temp_max'),
                'pressure': data.get('main', {}).get('pressure'),
                'humidity': data.get('main', {}).get('humidity'),
                'wind_speed': data.get('wind', {}).get('speed'),
                'wind_direction': data.get('wind', {}).get('deg'),
                'cloudiness': data.get('clouds', {}).get('all'),
                'weather_main': data.get('weather', [{}])[0].get('main'),
                'weather_description': data.get('weather', [{}])[0].get('description'),
                'rain_1h': data.get('rain', {}).get('1h', 0),
                'snow_1h': data.get('snow', {}).get('1h', 0),
                'visibility': data.get('visibility'),
                'sunrise': datetime.fromtimestamp(data.get('sys', {}).get('sunrise', 0)).isoformat() if data.get('sys', {}).get('sunrise') else None,
                'sunset': datetime.fromtimestamp(data.get('sys', {}).get('sunset', 0)).isoformat() if data.get('sys', {}).get('sunset') else None,
                'timezone': data.get('timezone'),
                'extraction_timestamp': data.get('extraction_timestamp'),
                'processing_timestamp': datetime.now().isoformat()
            }
            
            
            # Classificação de temperatura
            temp = transformed_item['temperature']
            if temp is not None:
                if temp < 10:
                    transformed_item['temp_category'] = 'cold'
                elif temp < 20:
                    transformed_item['temp_category'] = 'mild'
                elif temp < 30:
                    transformed_item['temp_category'] = 'warm'
                else:
                    transformed_item['temp_category'] = 'hot'
            
            transformed_data.append(transformed_item)
            
        except Exception as e:
            print(f"Erro ao transformar dados: {str(e)}")
    
    
    df = pd.DataFrame(transformed_data)
    
    # Realiza limpeza de dados
    # Preenche valores nulos em campos numéricos
    numeric_columns = ['temperature', 'feels_like', 'temp_min', 'temp_max', 
                       'pressure', 'humidity', 'wind_speed', 'wind_direction', 
                       'cloudiness', 'rain_1h', 'snow_1h', 'visibility']
    
    df[numeric_columns] = df[numeric_columns].fillna(0)
    
   
    df = df.drop_duplicates(subset=['city_id', 'extraction_timestamp'])
    
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"temp/weather_transformed_{timestamp}.csv"
    df.to_csv(output_file, index=False)
    
    print(f"Transformação concluída. Dados salvos em: {output_file}")
    
    return output_file
