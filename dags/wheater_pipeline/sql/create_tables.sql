-- Tabela principal para dados meteorológicos
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city_id INTEGER,
    city_name VARCHAR(100),
    country VARCHAR(5),
    latitude FLOAT,
    longitude FLOAT,
    temperature FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure FLOAT,
    humidity FLOAT,
    wind_speed FLOAT,
    wind_direction FLOAT,
    cloudiness INTEGER,
    weather_main VARCHAR(50),
    weather_description TEXT,
    rain_1h FLOAT,
    snow_1h FLOAT,
    visibility INTEGER,
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    timezone INTEGER,
    temp_category VARCHAR(20),
    extraction_timestamp TIMESTAMP,
    processing_timestamp TIMESTAMP
);

-- Índices para melhorar performance das consultas
CREATE INDEX IF NOT EXISTS idx_weather_data_city_id ON weather_data(city_id);
CREATE INDEX IF NOT EXISTS idx_weather_data_city_name ON weather_data(city_name);
CREATE INDEX IF NOT EXISTS idx_weather_data_extraction_time ON weather_data(extraction_timestamp);

-- Tabela para resumo dos dados (usada pelo Gemini)
CREATE TABLE IF NOT EXISTS weather_summary (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100),
    country VARCHAR(5),
    temperature FLOAT,
    humidity FLOAT,
    weather_main VARCHAR(50),
    weather_description TEXT,
    temp_category VARCHAR(20)
);

-- Tabela para armazenar os insights gerados pelo Gemini
CREATE TABLE IF NOT EXISTS weather_insights (
    id SERIAL PRIMARY KEY,
    insight_type VARCHAR(50),
    insight_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
