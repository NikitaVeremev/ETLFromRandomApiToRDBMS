from airflow import DAG
from airflow.utils.dates import days_ago

from scripts.api_to_clickhouse import ApiToClickhouseOperator
from scripts.api_to_postgres import ApiToPostgresOperator

dag_id = 'from_api_to_rdbms'

dag = DAG(dag_id,
          schedule_interval='0 */12 * * *',
          start_date=days_ago(2),
          catchup=False)

target_table_ddl_ch = '''
        CREATE TABLE IF NOT EXISTS cannabis_api_summary (
          id UInt64,
          uid String,
          strain String,
          cannabinoid_abbreviation String,
          cannabinoid String,
          terpene String,
          medical_use String,
          category String,
          health_benefit String,
          type String,
          buzzword String,
          brand String
        ) ENGINE = MergeTree()
        ORDER BY id
        '''

api_to_postgres = ApiToPostgresOperator(
    task_id='from_api_to_postgres',
    api_url="https://random-data-api.com/api/cannabis/random_cannabis?size=10",
    db_url='postgresql://username:password@localhost:5432/mydatabase',
    target_table_name='cannabis_api_summary',
    dag=dag)

api_to_clickhouse = ApiToClickhouseOperator(
    task_id='from_api_to_clickhouse',
    api_url="https://random-data-api.com/api/cannabis/random_cannabis?size=10",
    db_url='clickhouse://test:test@localhost:56051/test',
    target_table_name='cannabis_api_summary',
    ddl=target_table_ddl_ch,
    dag=dag)

api_to_postgres
api_to_clickhouse
