# Credenciais e configurações
OPENWEATHER_API_KEY = "baae9dd9944633532474f8ad4062b3d5"
GEMINI_API_KEY = "AIzaSyC99DM85FQnh0IbEOb2tm-S7TTjDSI5csA"

# Conexão PostgreSQL
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_HOST = "host.docker.internal"  # Nome do serviço no Docker Compose
POSTGRES_PORT = "5433"
POSTGRES_DB = "weather_db"

POSTGRES_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Lista de cidades para monitorar
CITIES = [
    "São Paulo",
    "Rio de Janeiro",
    "Brasília",
    "Salvador",
    "Fortaleza",
    "Belo Horizonte",
    "Manaus",
    "Curitiba",
    "Recife",
    "Porto Alegre",
    "Belém",
    "Goiânia",
    "Guarulhos",
    "Campinas",
    "São Luís"
]
