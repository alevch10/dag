from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models.xcom_arg import XComArg
from datetime import datetime, timedelta
import logging

EXCLUDED_SCHEMAS = ["amplitude_mob", "amplitude_web"]


def get_all_tables():
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    sql = """
        SELECT table_schema || '.' || table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('information_schema', 'pg_catalog')
          AND table_schema NOT IN %(excluded_schemas)s
        ORDER BY table_schema, table_name
    """
    rows = hook.get_records(
        sql, parameters={"excluded_schemas": tuple(EXCLUDED_SCHEMAS)}
    )
    tables = [row[0] for row in rows]
    logging.info(f"Найдено таблиц для обслуживания: {len(tables)}")
    # Возвращаем список словарей, каждый с table_name и use_concurrently
    return [{"table_name": t, "use_concurrently": True} for t in tables]


def maintain_table(table_name: str, use_concurrently: bool = True):
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    logging.info(f"Начинаем обслуживание таблицы {table_name}")
    try:
        logging.info(f"VACUUM {table_name}")
        hook.run(f"VACUUM {table_name};")
        logging.info(f"VACUUM {table_name} завершён")

        logging.info(f"ANALYZE {table_name}")
        hook.run(f"ANALYZE {table_name};")
        logging.info(f"ANALYZE {table_name} завершён")

        reindex_cmd = (
            f"REINDEX TABLE CONCURRENTLY {table_name};"
            if use_concurrently
            else f"REINDEX TABLE {table_name};"
        )
        logging.info(f"REINDEX {table_name} (CONCURRENTLY={use_concurrently})")
        hook.run(reindex_cmd)
        logging.info(f"REINDEX {table_name} завершён")

        logging.info(f"Обслуживание таблицы {table_name} успешно завершено")
    except Exception as e:
        logging.error(f"Ошибка при обслуживании таблицы {table_name}: {e}")
        raise


default_args = {
    "owner": "admin",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
    "catchup": False,
}

with DAG(
    dag_id="db_maintenance",
    default_args=default_args,
    schedule="0 0 1 * *",
    description="Ежемесячное обслуживание PostgreSQL: VACUUM, ANALYZE, REINDEX (кроме amplitude_*)",
    tags=["maintenance", "postgres"],
    max_active_tasks=1,  # Задачи выполняются последовательно
) as dag:
    get_tables = PythonOperator(
        task_id="get_tables",
        python_callable=get_all_tables,
    )

    # Создаём задачи для каждой таблицы, передавая op_kwargs через expand
    maintain_tasks = PythonOperator.partial(
        task_id="maintain_table",
        python_callable=maintain_table,
    ).expand(op_kwargs=XComArg(get_tables))

    get_tables >> maintain_tasks
