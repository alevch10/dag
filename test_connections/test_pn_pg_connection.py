from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


@dag(
    dag_id="test_pn_pg_connection",
    schedule=None,
    start_date=datetime(2026, 7, 28),
    catchup=False,
)
def test_pg_connection2():
    @task
    def test():
        hook = PostgresHook(postgres_conn_id="pn_pg")

        print("CONNECTION:")
        print(hook.get_connection("pn_pg"))

        print("NOW:")
        print(hook.get_first("select now();"))

    test()


test_pg_connection2()
