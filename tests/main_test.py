import clickhouse_driver
import sqlalchemy
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.postgres import PostgresContainer

from scripts.api_to_clickhouse import ApiToClickhouseOperator
from scripts.api_to_postgres import ApiToPostgresOperator
from dags.random_api_to_rdbms import target_table_ddl_ch


def test_api_to_postgres_rowcount():
    with PostgresContainer("postgres:9.5") as postgres:
        client = sqlalchemy.create_engine(postgres.get_connection_url())
        op = ApiToPostgresOperator(
            task_id='from_api_to_postgres',
            api_url="https://random-data-api.com/api/cannabis/random_cannabis?size=10",
            db_url=postgres.get_connection_url(),
            target_table_name='cannabis_api_summary')
        op.build()
        result = client.execute("select * from public.cannabis_api_summary")
    assert result.rowcount == 10


def test_api_to_clickhouse_rowcount():
    with ClickHouseContainer() as clickhouse:
        client = clickhouse_driver.Client.from_url(clickhouse.get_connection_url())
        client.execute("select 'working'")
        op = ApiToClickhouseOperator(
            task_id='from_api_to_clickhouse',
            api_url="https://random-data-api.com/api/cannabis/random_cannabis?size=10",
            db_url=clickhouse.get_connection_url(),
            ddl=target_table_ddl_ch,
            target_table_name='cannabis_api_summary')
        op.build()
        result = client.execute("select * from cannabis_api_summary")
    assert len(result) == 10
