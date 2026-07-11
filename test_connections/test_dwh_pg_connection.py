from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


@dag(
    dag_id="test_dwh_pg_connection",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def test_pg_connection2():
    @task
    def test():
        hook = PostgresHook(postgres_conn_id="dwh_pg")

        print("CONNECTION:")
        print(hook.get_connection("dwh_pg"))

        print("NOW:")
        print(hook.get_first("select now();"))

    test()


test_pg_connection2()
