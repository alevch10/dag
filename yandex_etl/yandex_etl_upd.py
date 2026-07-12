from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.utils import timezone

from datetime import datetime, timedelta

import requests
import logging
import os


def load_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_yaml(filename):
    return load_file(
        os.path.join(os.path.dirname(__file__), "yaml_templates", filename)
    )


def load_sql(filename):
    return load_file(os.path.join(os.path.dirname(__file__), "sql", filename))


def load_source(table_name, date_field, yaml_template, source_name, **context):
    hook = PostgresHook(postgres_conn_id="dwh_pg")

    sql = f"""
        SELECT max({date_field})
        FROM {table_name}
    """
    result = hook.get_first(sql)
    max_date = result[0] if result else None

    today = timezone.utcnow().date()
    yesterday = today - timedelta(days=1)

    if max_date:
        if isinstance(max_date, datetime):
            max_date = max_date.date()
        if max_date >= yesterday:
            raise AirflowSkipException(f"{source_name}: данные за {yesterday} уже есть")
        start_date = max_date + timedelta(days=1)
    else:
        start_date = yesterday - timedelta(days=7)

    end_date = yesterday

    payload = yaml_template.format(
        start_date=start_date.isoformat(), end_date=end_date.isoformat()
    )

    logging.info(
        f"""
        =========================
        ETL START
        Source: {source_name}
        Period: {start_date} - {end_date}
        =========================
        """
    )

    response = requests.post(
        f"{Variable.get('dwh_helper_url')}/etl/transformer?start_after_line=0",
        data=payload,
        headers={
            "Authorization": f"Bearer {Variable.get('BEARER')}",
            "Content-Type": "application/x-yaml",
        },
        timeout=72000,
        verify=False,
    )
    response.raise_for_status()
    result = response.json()

    logging.info(f"{source_name} result: {result}")

    if result.get("status") != "success":
        raise Exception(result)

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }


def execute_sql(sql, source_task, **context):
    ti = context["ti"]
    dates = ti.xcom_pull(task_ids=source_task)

    if not dates:
        raise Exception(f"No dates from {source_task}")

    hook = PostgresHook(postgres_conn_id="dwh_pg")

    logging.info(
        f"""
        SQL execution
        source task: {source_task}
        period: {dates}
        """
    )

    hook.run(
        sql,
        parameters={
            "start_date": dates["start_date"],
            "end_date": dates["end_date"],
        },
    )


default_args = {
    "owner": "levchenko-an",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="kpi_user_activity",
    start_date=datetime(2026, 7, 12),
    schedule="0 3 * * *",
    catchup=False,
    default_args=default_args,
    tags=["kpi", "user_activity"],
) as dag:
    # -------------------------
    # AppMetrica
    # -------------------------
    load_appmetrica = PythonOperator(
        task_id="load_appmetrica",
        python_callable=load_source,
        op_kwargs={
            "table_name": "appmetrica.events",
            "date_field": "event_datetime",
            "yaml_template": load_yaml("appmetrica.yaml"),
            "source_name": "AppMetrica",
        },
    )

    identity_appmetrica = PythonOperator(
        task_id="build_identity_appmetrica",
        python_callable=execute_sql,
        op_kwargs={
            "sql": load_sql("update_identity_appmetrica.sql"),
            "source_task": "load_appmetrica",
        },
    )

    activity_appmetrica = PythonOperator(
        task_id="build_activity_appmetrica",
        python_callable=execute_sql,
        op_kwargs={
            "sql": load_sql("load_activity_appmetrica.sql"),
            "source_task": "load_appmetrica",
        },
    )

    # -------------------------
    # Booking
    # -------------------------
    load_booking = PythonOperator(
        task_id="load_booking",
        python_callable=load_source,
        op_kwargs={
            "table_name": "yandex_metrika_booking.events",
            "date_field": "date_time",
            "yaml_template": load_yaml("booking.yaml"),
            "source_name": "Booking",
        },
    )

    identity_booking = PythonOperator(
        task_id="build_identity_booking",
        python_callable=execute_sql,
        op_kwargs={
            "sql": load_sql("update_identity_booking.sql"),
            "source_task": "load_booking",
        },
    )

    activity_booking = PythonOperator(
        task_id="build_activity_booking",
        python_callable=execute_sql,
        op_kwargs={
            "sql": load_sql("load_activity_booking.sql"),
            "source_task": "load_booking",
        },
    )

    # -------------------------
    # Web LK
    # -------------------------
    load_web_lk = PythonOperator(
        task_id="load_web_lk",
        python_callable=load_source,
        op_kwargs={
            "table_name": "yandex_metrika_web_lk.events",
            "date_field": "date_time",
            "yaml_template": load_yaml("web_lk.yaml"),
            "source_name": "Web LK",
        },
    )

    identity_web_lk = PythonOperator(
        task_id="build_identity_web_lk",
        python_callable=execute_sql,
        op_kwargs={
            "sql": load_sql("update_identity_web_lk.sql"),
            "source_task": "load_web_lk",
        },
    )

    activity_web_lk = PythonOperator(
        task_id="build_activity_web_lk",
        python_callable=execute_sql,
        op_kwargs={
            "sql": load_sql("load_activity_web_lk.sql"),
            "source_task": "load_web_lk",
        },
    )

    # -------------------------
    # Общая последовательность
    # -------------------------
    (
        load_appmetrica
        >> identity_appmetrica
        >> activity_appmetrica
        >> load_booking
        >> identity_booking
        >> activity_booking
        >> load_web_lk
        >> identity_web_lk
        >> activity_web_lk
    )
