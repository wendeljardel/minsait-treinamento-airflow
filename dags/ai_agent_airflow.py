"""
DAG que implementa um agente AI usando Pydantic e Gemini para interagir com o Airflow.
Este agente pode analisar e fornecer insights sobre as DAGs em execução.
"""

from datetime import datetime, timedelta
from typing import List, Optional
from pydantic import BaseModel, Field
import google.generativeai as genai
from airflow.decorators import dag, task
from airflow.models import DagRun
from airflow.utils.db import provide_session
from sqlalchemy.orm import Session
from airflow.utils.session import create_session

# Configuração do Gemini
GOOGLE_API_KEY = "AIzaSyC99DM85FQnh0IbEOb2tm-S7TTjDSI5csA"  # Substitua pela sua chave API
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

class DagRunInfo(BaseModel):
    """Modelo Pydantic para informações da execução da DAG"""
    dag_id: str
    execution_date: datetime
    state: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    duration: Optional[timedelta] = None

class DagAnalysis(BaseModel):
    """Modelo Pydantic para análise da DAG"""
    dag_id: str
    total_runs: int
    success_rate: float
    average_duration: timedelta
    last_status: str
    recommendations: List[str] = Field(default_factory=list)

@dag(
    dag_id='ai_agent_airflow',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ai', 'analysis', 'gemini']
)
def ai_agent_airflow():

    @task()
    def get_dag_runs() -> List[dict]:
        """Obtém informações sobre as execuções das DAGs"""
        with create_session() as session:
            dag_runs = session.query(DagRun).all()

            dag_run_info_list = []
            for run in dag_runs:
                duration = None
                if run.start_date and run.end_date:
                    duration = run.end_date - run.start_date

                dag_run_info = DagRunInfo(
                    dag_id=run.dag_id,
                    execution_date=run.execution_date,
                    state=run.state,
                    start_date=run.start_date,
                    end_date=run.end_date,
                    duration=duration
                )
                dag_run_info_list.append(dag_run_info)

            
            return [run.dict() for run in dag_run_info_list]


    @task()
    def analyze_dags(dag_runs: List[dict]) -> List[dict]:
        """Analisa as DAGs usando o modelo Gemini"""
        dag_analyses = []
        
        dag_runs = [DagRunInfo(**run) for run in dag_runs]
        # Agrupa execuções por DAG
        dag_groups = {}
        for run in dag_runs:
            if run.dag_id not in dag_groups:
                dag_groups[run.dag_id] = []
            dag_groups[run.dag_id].append(run)
        
        # Analisa cada DAG
        for dag_id, runs in dag_groups.items():
            total_runs = len(runs)
            successful_runs = sum(1 for run in runs if run.state == 'success')
            success_rate = (successful_runs / total_runs) * 100 if total_runs > 0 else 0
            
            # Calcula duração média
            durations = [run.duration for run in runs if run.duration]
            avg_duration = sum(durations, timedelta()) / len(durations) if durations else timedelta()
            
            # Gera recomendações usando Gemini
            prompt = f"""
            Analise o seguinte cenário de execução de DAG:
            - DAG ID: {dag_id}
            - Total de execuções: {total_runs}
            - Taxa de sucesso: {success_rate:.2f}%
            - Duração média: {avg_duration}
            
            Forneça 3 recomendações para melhorar o desempenho e confiabilidade desta DAG.
            """
            
            response = model.generate_content(prompt)
            # Verifica se houve retorno válido
            if response.candidates and response.candidates[0].content.parts:
                recommendations = response.text.split('\n')
            else:
                recommendations = ["Sem recomendações disponíveis no momento."]
            recommendations = response.text.split('\n')
            
            analysis = DagAnalysis(
                dag_id=dag_id,
                total_runs=total_runs,
                success_rate=success_rate,
                average_duration=avg_duration,
                last_status=runs[-1].state if runs else 'unknown',
                recommendations=recommendations
            )
            
            dag_analyses.append(analysis)
        
        return dag_analyses

    @task()
    def generate_report(analyses: List[DagAnalysis]) -> str:
        """Gera um relatório detalhado das análises"""
        report = "Relatório de Análise de DAGs\n\n"
        
        for analysis in analyses:
            report += f"DAG: {analysis.dag_id}\n"
            report += f"Total de Execuções: {analysis.total_runs}\n"
            report += f"Taxa de Sucesso: {analysis.success_rate:.2f}%\n"
            report += f"Duração Média: {analysis.average_duration}\n"
            report += f"Último Status: {analysis.last_status}\n"
            report += "\nRecomendações:\n"
            for rec in analysis.recommendations:
                report += f"- {rec}\n"
            report += "\n" + "="*50 + "\n\n"
        
        return report

    # Definindo o fluxo de execução
    dag_runs = get_dag_runs()
    analyses = analyze_dags(dag_runs)
    report = generate_report(analyses)

# Criando a DAG
ai_agent_dag = ai_agent_airflow() 