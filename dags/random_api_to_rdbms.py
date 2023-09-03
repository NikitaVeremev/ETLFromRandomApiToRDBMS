from airflow import DAG
from airflow.models import Connection
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook

from scripts.api_to_clickhouse import ApiToClickhouseOperator
from scripts.api_to_postgres import ApiToPostgresOperator

dag_id = "from_api_to_rdbms"

psql_conn: Connection = BaseHook.get_connection("postgres_connection")
ch_conn: Connection = BaseHook.get_connection("clickhouse_connection")

api_url = "https://random-data-api.com/api/cannabis/random_cannabis?size=10"
target_table_name = "cannabis_api_summary"

target_table_ddl_ch = f"""
        CREATE TABLE IF NOT EXISTS {target_table_name} (
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
        """

with DAG(
    dag_id, schedule_interval="0 */12 * * *", start_date=days_ago(2), catchup=False
) as dag:
    api_to_postgres = ApiToPostgresOperator(
        task_id="from_api_to_postgres",
        api_url=api_url,
        db_url=psql_conn.get_uri(),
        target_table_name=target_table_name,
        dag=dag,
    )

    api_to_clickhouse = ApiToClickhouseOperator(
        task_id="from_api_to_clickhouse",
        api_url=api_url,
        db_url=ch_conn.get_uri(),
        target_table_name=target_table_name,
        ddl=target_table_ddl_ch,
        dag=dag,
    )

    api_to_postgres >> api_to_clickhouse
