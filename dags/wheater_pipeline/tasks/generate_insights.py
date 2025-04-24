import pandas as pd
from sqlalchemy import create_engine
import google.generativeai as genai
from airflow.decorators import task
from wheater_pipeline.config.config import POSTGRES_CONN_STRING, GEMINI_API_KEY

@task
def generate_gemini_insights(db_confirmation: str):
    """
    Gera insights sobre os dados meteorológicos usando a API Gemini.
    
    Args:
        db_confirmation (str): Confirmação de que os dados foram carregados no banco
        
    Returns:
        str: Os insights gerados pelo modelo Gemini
    """
    print("Iniciando geração de insights com a API Gemini...")
    
    # Configura a API Gemini
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    # Conecta ao PostgreSQL e obtém os dados de resumo
    engine = create_engine(POSTGRES_CONN_STRING)
    query = "SELECT * FROM weather_summary"
    df = pd.read_sql(query, engine)
    
    # Prepara os dados para enviar ao Gemini
    data_description = df.to_string()
    
    # Criar prompts específicos para obter insights valiosos
    prompts = [
        f"""Analise os seguintes dados meteorológicos e identifique padrões ou correlações interessantes. 
        Foque em relações entre temperatura, umidade e condições climáticas:
        {data_description}""",
        
        f"""Com base nos dados meteorológicos a seguir, identifique cidades com condições climáticas 
        extremas ou incomuns e explique o porquê:
        {data_description}""",
        
        f"""Analise os seguintes dados meteorológicos e sugira como essas condições podem impactar 
        atividades econômicas, turismo ou qualidade de vida nas cidades listadas:
        {data_description}"""
    ]
    
    # Gera insights para cada prompt
    insights = []
    for i, prompt in enumerate(prompts):
        try:
            response = model.generate_content(prompt)
            insight = response.text
            insights.append(f"Insight {i+1}:\n{insight}")
            print(f"Insight {i+1} gerado com sucesso")
        except Exception as e:
            print(f"Erro ao gerar insight {i+1}: {str(e)}")
            insights.append(f"Insight {i+1}: Erro na geração - {str(e)}")
    
    # Salva os insights em uma tabela no PostgreSQL
    insights_df = pd.DataFrame({
        'insight_type': ['padrões', 'condições_extremas', 'impactos_sociais'],
        'insight_text': insights
    })
    
    try:
        insights_df.to_sql('weather_insights', engine, if_exists='replace', index=False)
        print("Insights salvos na tabela 'weather_insights'")
    except Exception as e:
        print(f"Erro ao salvar insights no PostgreSQL: {str(e)}")
    
    return "\n\n".join(insights)
