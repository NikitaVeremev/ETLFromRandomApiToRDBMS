import sqlalchemy
from testcontainers.postgres import PostgresContainer

from dags.random_api_to_rdbms import build_etl


def test_main():
    with PostgresContainer("postgres:9.5") as postgres:
        e = sqlalchemy.create_engine(postgres.get_connection_url())
        build_etl(postgres.get_connection_url())
        result = e.execute("select * from public.cannabis_api_summary")
        assert result.rowcount == 10
