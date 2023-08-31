from airflow import DAG
from airflow.utils.dates import days_ago

from scripts.api_to_postgres import ApiToPostgresOperator

dag_id = 'from_api_to_rdbms'

dag = DAG(dag_id,
          schedule_interval='0 */12 * * *',
          start_date=days_ago(2),
          catchup=False)

api_to_postgres = ApiToPostgresOperator(
    task_id='from_api_to_postgres',
    api_url="https://random-data-api.com/api/cannabis/random_cannabis?size=10",
    db_url='postgresql://username:password@localhost:5432/mydatabase',
    target_table_name='cannabis_api_summary',
    dag=dag)

api_to_postgres
