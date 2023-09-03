#!/bin/sh
airflow db init
airflow connections add "postgres_connection" --conn-type 'postgresql'
airflow connections add "clickhouse_connection" --conn-type 'clickhouse'
