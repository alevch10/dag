import logging
import time

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
from psycopg2.extras import execute_values


GET_LAST_ONLINE_APPOINTMENT = """
SELECT
    create_dt
FROM
    kpi.online_appointments
ORDER BY 1 DESC
LIMIT 1
"""

GET_ONLINE_APPOINTMENTS = """
SELECT
    created_at AS create_dt,
    appointment_oid AS appointment_id,
    source
FROM
    sgm_schedule.appointment_record
WHERE
    created_at >= %s
    AND created_at <  %s
"""

INSERT_ONLINE_APPOINTMENTS = """
INSERT INTO kpi.online_appointments (create_dt, appointment_id, "source")
VALUES %s
ON CONFLICT (appointment_id) DO NOTHING
"""


def add_month(dt: datetime) -> datetime:
    """Прибавляет один месяц к datetime, сохраняя день/время."""
    if dt.month == 12:
        return dt.replace(year=dt.year + 1, month=1)
    return dt.replace(month=dt.month + 1)


def get_last_online_appointment_date() -> datetime:
    """Возвращает последнюю дату из kpi.online_appointments (или 2025-01-01 00:00:00)."""
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    result = hook.get_first(GET_LAST_ONLINE_APPOINTMENT)
    logging.info(f"Last online appointment date: {result}")
    if result is None or result[0] is None:
        return datetime(2025, 1, 1, 0, 0, 0)
    return result[0]


def get_online_appointments(start_date: datetime, end_date: datetime) -> list[tuple]:
    """Получаем записи из sgm_schedule.appointment_record за период [start_date, end_date)."""
    hook = PostgresHook(postgres_conn_id="esb_db")
    result = hook.get_records(
        GET_ONLINE_APPOINTMENTS, parameters=(start_date, end_date)
    )
    logging.info(
        f"Online appointments from {start_date} to {end_date}: {len(result)} records found"
    )
    return result


def load_online_appointments(data: list[tuple]):
    """Вставляем записи в kpi.online_appointments."""
    if not data:
        logging.info("No records to insert into kpi.online_appointments")
        return
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    conn = hook.get_conn()
    cursor = conn.cursor()
    execute_values(cursor, INSERT_ONLINE_APPOINTMENTS, data, page_size=10000)
    inserted = cursor.rowcount
    conn.commit()
    cursor.close()
    logging.info(f"Inserted {inserted} new records into kpi.online_appointments")


def insert_online_appointments():
    """
    Основной процесс:
    1. Берём последнюю дату из kpi.online_appointments (или 2025-01-01 00:00:00).
    2. В цикле выгружаем из sgm_schedule.appointment_record кусками по одному месяцу.
    3. Пишем данные в kpi.online_appointments.
    4. Ждём 10 секунд между итерациями, чтобы не спамить БД.
    Продолжаем, пока не дойдём до текущего момента.
    """
    start_date = get_last_online_appointment_date()

    # Определяем "сейчас" с учётом таймзоны start_date (если она есть)
    now = datetime.now(tz=start_date.tzinfo) if start_date.tzinfo else datetime.now()

    if start_date >= now:
        logging.info(
            f"Last date {start_date} is in the future or now ({now}). Nothing to process."
        )
        return

    logging.info(f"Processing online appointments from {start_date} up to {now}")

    while start_date < now:
        end_date = add_month(start_date)
        if end_date > now:
            end_date = now

        try:
            records = get_online_appointments(start_date, end_date)
            load_online_appointments(records)
        except Exception:
            logging.exception(
                f"Failed to process period {start_date} – {end_date}, aborting loop"
            )
            raise

        start_date = end_date

        # Пауза, чтобы не спамить esb_db (даже если данных не было)
        time.sleep(10)

    logging.info("All online appointments up to current date have been processed.")


default_args = {
    "owner": "levchenko-an",
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="patientnet_online_appointments_etl",
    start_date=datetime(2026, 7, 27),
    schedule="0 1 * * *",
    catchup=False,
    default_args=default_args,
    tags=["kpi", "patientnet", "online_appointments"],
) as dag:
    insert_online_appointments_task = PythonOperator(
        task_id="insert_online_appointments",
        python_callable=insert_online_appointments,
    )