from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def test_pg_connection():

    @task
    def test():
        hook = PostgresHook(postgres_conn_id="dwh_pg")

        print(
            hook.get_first(
                "select max(date_time) from yandex_metrika_booking.events"
            )
        )

    test()


test_pg_connection()
