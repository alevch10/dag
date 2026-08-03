from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


@dag(
    dag_id="test_both_pg_connections",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def test_both_pg_connections():
    @task
    def test_dwh_pg():
        hook = PostgresHook(postgres_conn_id="dwh_pg")
        print("CONNECTION dwh_pg:")
        print(hook.get_connection("dwh_pg"))
        print("NOW dwh_pg:")
        print(hook.get_first("select now();"))

    @task
    def test_pn_pg():
        hook = PostgresHook(postgres_conn_id="pn_pg")
        print("CONNECTION pn_pg:")
        print(hook.get_connection("pn_pg"))
        print("NOW pn_pg:")
        print(hook.get_first("select now();"))

    # Последовательное выполнение
    test_dwh_pg() >> test_pn_pg()


# Инстанцирование DAG
test_both_pg_connections()
