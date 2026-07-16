from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.sdk import Variable, timezone
from airflow.exceptions import AirflowSkipException

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


PRESENCE_SQL = """
INSERT INTO kpi.user_presence
(
    activity_date,
    source,
    product,
    platform,
    internal_user_id,
    external_user_id,
    first_event_time
)
SELECT
    DATE(e.{date_field}),
    '{source}',
    '{product}',
    '{platform}',
    MAX(ui.internal_user_id),
    {external_id},
    MIN(e.{date_field})
FROM {table_name} e
LEFT JOIN kpi.user_identity ui
ON ui.source = '{identity_source}'
AND ui.external_user_id::TEXT = {external_id}
{joins}
WHERE
    {where_clause}
    AND e.{date_field} >= '{start_date}'::date
    AND e.{date_field} < ('{end_date}'::date + interval '1 day')
GROUP BY
    DATE(e.{date_field}),
    {external_id}
ON CONFLICT
(
    activity_date,
    source,
    product,
    external_user_id
)
DO UPDATE
SET
    internal_user_id =
        COALESCE(
            EXCLUDED.internal_user_id,
            kpi.user_presence.internal_user_id
        ),
    updated_at = now();
"""


def execute_sql(sql, source_task, **context):
    ti = context["ti"]
    dates = ti.xcom_pull(task_ids=source_task)
    if not dates:
        raise Exception(f"No dates from {source_task}")

    # Форматируем SQL с подстановкой дат
    formatted_sql = sql.format(**dates)

    hook = PostgresHook(postgres_conn_id="dwh_pg")
    logging.info(
        f"""
        SQL execution
        source task: {source_task}
        period: {dates}
        SQL: {formatted_sql}
        """
    )
    hook.run(formatted_sql)


default_args = {
    "owner": "levchenko-an",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def execute_presence(sql, source_task, **context):
    ti = context["ti"]
    dates = ti.xcom_pull(task_ids=source_task)
    if not dates:
        raise Exception(f"No dates from {source_task}")

    formatted_sql = sql.format(**dates)

    hook = PostgresHook(postgres_conn_id="dwh_pg")
    logging.info(
        f"""
        =========================
        BUILD USER PRESENCE
        source task: {source_task}
        period: {dates}
        SQL: {formatted_sql}
        =========================
        """
    )
    hook.run(formatted_sql)
    logging.info(
        f"""
        USER PRESENCE COMPLETED
        period: {dates["start_date"]} - {dates["end_date"]}
        """
    )


with DAG(
    dag_id="yandex_metrika_data_and_mau_kpis",
    start_date=datetime(2026, 7, 15),
    schedule="0 3 * * *",
    catchup=False,
    default_args=default_args,
    tags=[
        "kpi",
        "appmetrica",
        "yandex_metrika",
        "mau",
        "lk",
        "oz",
    ],
) as dag:
    # ==========================================================
    # Источники данных
    # ==========================================================

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

    # ==========================================================
    # Identity
    # ==========================================================

    identity_configs = [
        {
            "task_id": "build_identity_appmetrica",
            "sql": load_sql("update_identity_appmetrica.sql"),
            "source_task": "load_appmetrica",
        },
        {
            "task_id": "build_identity_booking",
            "sql": load_sql("update_identity_booking.sql"),
            "source_task": "load_booking",
        },
        {
            "task_id": "build_identity_web_lk",
            "sql": load_sql("update_identity_web_lk.sql"),
            "source_task": "load_web_lk",
        },
    ]

    identity_tasks = {}

    for cfg in identity_configs:
        identity_tasks[cfg["task_id"]] = PythonOperator(
            task_id=cfg["task_id"],
            python_callable=execute_sql,
            op_kwargs={
                "sql": cfg["sql"],
                "source_task": cfg["source_task"],
            },
        )

    # ==========================================================
    # Presence
    # ==========================================================

    presence_configs = [
        # ---------- AppMetrica OZ ----------
        {
            "task_id": "presence_appmetrica_oz",
            "source_task": "load_appmetrica",
            "table_name": "appmetrica.events",
            "date_field": "event_datetime",
            "source": "appmetrica",
            "identity_source": "appmetrica",
            "product": "oz",
            "platform": "mobile",
            "external_id": "e.appmetrica_device_id::text",
            "joins": """
                JOIN kpi.oz_events_mobile o
                ON o.event_type=e.event_name
            """,
            "where_clause": "TRUE",
        },
        # ---------- AppMetrica LK ----------
        {
            "task_id": "presence_appmetrica_lk",
            "source_task": "load_appmetrica",
            "table_name": "appmetrica.events",
            "date_field": "event_datetime",
            "source": "appmetrica",
            "identity_source": "appmetrica",
            "product": "lk",
            "platform": "mobile",
            "external_id": "e.appmetrica_device_id::text",
            "joins": "",
            "where_clause": "TRUE",
        },
        # ---------- Booking ----------
        {
            "task_id": "presence_booking",
            "source_task": "load_booking",
            "table_name": "yandex_metrika_booking.events",
            "date_field": "date_time",
            "source": "booking",
            "identity_source": "booking",
            "product": "oz",
            "platform": "web",
            "external_id": "e.client_id::text",
            "joins": "",
            "where_clause": """
                e.is_page_view=false
                AND e.url LIKE 'goal://booking.avaclinic.ru/%'
            """,
        },
        # ---------- Web LK ----------
        {
            "task_id": "presence_web_lk",
            "source_task": "load_web_lk",
            "table_name": "yandex_metrika_web_lk.events",
            "date_field": "date_time",
            "source": "web_lk",
            "identity_source": "web_lk",
            "product": "lk",
            "platform": "web",
            "external_id": "e.client_id::text",
            "joins": "",
            "where_clause": """
                e.is_page_view=true
            """,
        },
    ]

    presence_tasks = {}

    for cfg in presence_configs:
        presence_tasks[cfg["task_id"]] = PythonOperator(
            task_id=cfg["task_id"],
            python_callable=execute_presence,
            op_kwargs={
                "source_task": cfg["source_task"],
                "sql": PRESENCE_SQL.format(**cfg),
            },
        )

    # ==========================================================
    # Dependencies
    # ==========================================================

    load_appmetrica >> identity_tasks["build_identity_appmetrica"]

    identity_tasks["build_identity_appmetrica"] >> [
        presence_tasks["presence_appmetrica_oz"],
        presence_tasks["presence_appmetrica_lk"],
    ]

    (presence_tasks["presence_appmetrica_oz"] >> load_booking)

    (presence_tasks["presence_appmetrica_lk"] >> load_booking)

    load_booking >> identity_tasks["build_identity_booking"]

    identity_tasks["build_identity_booking"] >> presence_tasks["presence_booking"]

    presence_tasks["presence_booking"] >> load_web_lk

    load_web_lk >> identity_tasks["build_identity_web_lk"]

    identity_tasks["build_identity_web_lk"] >> presence_tasks["presence_web_lk"]
