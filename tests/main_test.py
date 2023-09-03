import clickhouse_driver
import sqlalchemy
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.postgres import PostgresContainer

from dags.random_api_to_rdbms import api_to_postgres, api_url, api_to_clickhouse, target_table_ddl_ch, target_table_name
from scripts.api_to_clickhouse import ApiToClickhouseOperator
from scripts.api_to_postgres import ApiToPostgresOperator


def test_api_to_postgres_rowcount():
    with PostgresContainer("postgres:9.5") as postgres:
        client = sqlalchemy.create_engine(postgres.get_connection_url())
        op = ApiToPostgresOperator(
            task_id=api_to_postgres.task_id,
            api_url=api_url,
            db_url=postgres.get_connection_url(),
            target_table_name=target_table_name)
        op.execute({})
        result = client.execute(f"select * from public.{target_table_name}")
    assert result.rowcount == 10


def test_api_to_clickhouse_rowcount():
    with ClickHouseContainer() as clickhouse:
        client = clickhouse_driver.Client.from_url(clickhouse.get_connection_url())
        op = ApiToClickhouseOperator(
            task_id=api_to_clickhouse.task_id,
            api_url=api_url,
            db_url=clickhouse.get_connection_url(),
            ddl=target_table_ddl_ch,
            target_table_name=target_table_name)
        op.execute({})
        result = client.execute(f"select * from {target_table_name}")
    assert len(result) == 10
