import clickhouse_driver
import sqlalchemy
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.postgres import PostgresContainer

from dags.random_api_to_rdbms import cannabis_api_to_postgres, cannabis_api_url, cannabis_api_to_clickhouse, \
    target_table_ddl_ch, target_table_name_cannabis, target_table_name_nhl, nhl_api_url, nhl_api_to_postgres
from scripts.api_to_clickhouse import ApiToClickhouseOperator
from scripts.api_to_postgres import ApiToPostgresOperator
from scripts.nhl_api_to_postgres import NHLApiToPostgresOperator


def test_api_to_postgres_rowcount():
    with PostgresContainer() as postgres:
        client = sqlalchemy.create_engine(postgres.get_connection_url())
        op = ApiToPostgresOperator(
            task_id=cannabis_api_to_postgres.task_id,
            api_url=cannabis_api_url,
            db_url=postgres.get_connection_url(),
            target_table_name=target_table_name_cannabis,
        )
        op.execute({})
        result = client.execute(f"select * from public.{target_table_name_cannabis}")
    assert result.rowcount == 10


def test_nhl_api_to_postgres_rowcount():
    with PostgresContainer() as postgres:
        client = sqlalchemy.create_engine(postgres.get_connection_url())
        op = NHLApiToPostgresOperator(
            task_id=nhl_api_to_postgres.task_id,
            api_url=nhl_api_url,
            db_url=postgres.get_connection_url(),
            target_table_name=target_table_name_nhl,
        )
        op.execute({})
        result = client.execute(f"select * from public.{target_table_name_nhl}")
    assert result.rowcount == 2


def test_api_to_clickhouse_rowcount():
    with ClickHouseContainer() as clickhouse:
        client = clickhouse_driver.Client.from_url(clickhouse.get_connection_url())
        op = ApiToClickhouseOperator(
            task_id=cannabis_api_to_clickhouse.task_id,
            api_url=cannabis_api_url,
            db_url=clickhouse.get_connection_url(),
            ddl=target_table_ddl_ch,
            target_table_name=target_table_name_cannabis,
        )
        op.execute({})
        result = client.execute(f"select * from {target_table_name_cannabis}")
    assert len(result) == 10
