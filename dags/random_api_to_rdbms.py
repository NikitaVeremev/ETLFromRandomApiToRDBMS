from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from scripts.main import ETLApiImpl


def build_etl(db_url: str = 'postgresql://username:password@localhost:5432/mydatabase'):
    etl = ETLApiImpl(api_url="https://random-data-api.com/api/cannabis/random_cannabis?size=10",
                     db_url=db_url,
                     target_table_name='cannabis_api_summary')
    return etl.build()


dag_id = 'from_api_to_rdbms'

dag = DAG(dag_id,
          schedule_interval='0 */12 * * *',
          start_date=days_ago(2),
          catchup=False)

api_to_postgres = PythonOperator(task_id='from_api_to_postgres', python_callable=build_etl, dag=dag)

api_to_postgres
