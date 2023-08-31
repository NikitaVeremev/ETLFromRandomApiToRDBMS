import sqlalchemy
from testcontainers.postgres import PostgresContainer

from scripts.api_to_postgres import ApiToPostgresOperator


def test_api_to_postgres_rowcount():
    with PostgresContainer("postgres:9.5") as postgres:
        e = sqlalchemy.create_engine(postgres.get_connection_url())
        op = ApiToPostgresOperator(
            task_id='from_api_to_postgres',
            api_url="https://random-data-api.com/api/cannabis/random_cannabis?size=10",
            db_url=postgres.get_connection_url(),
            target_table_name='cannabis_api_summary')
        op.build()
        result = e.execute("select * from public.cannabis_api_summary")
        assert result.rowcount == 10
