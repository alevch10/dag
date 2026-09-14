import logging
import time
from datetime import date, datetime, timedelta, timezone as dt_timezone

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from psycopg2.extras import execute_values

# ──────────────────────────────────────────────────────────────────────────────
# Константы
# ──────────────────────────────────────────────────────────────────────────────

# Глубина скользящего окна перезагрузки в днях.
# 45 дней покрывают прошлый месяц целиком плюс текущий — этого достаточно,
# чтобы поздние правки в ESB доехали, а случайный сбой одного дня
# исправился сам на следующем прогоне.
RELOAD_DAYS = 45

# Раньше этой даты данных нет
DATA_FLOOR = datetime(2025, 1, 1, 0, 0, 0, tzinfo=dt_timezone.utc)

# Пауза между месячными кусками, чтобы не нагружать esb_db
CHUNK_PAUSE_SECONDS = 10

# Допустимое расхождение объёма между ESB и хранилищем
RECONCILE_TOLERANCE = 0.02


# ──────────────────────────────────────────────────────────────────────────────
# SQL
# ──────────────────────────────────────────────────────────────────────────────

GET_LAST_ONLINE_APPOINTMENT = """
SELECT MAX(create_dt)
FROM kpi.online_appointments
"""

# Д-2: DISTINCT ON возвращает последнюю версию записи по updated_at,
# как это было в исходном ручном запросе.
GET_ONLINE_APPOINTMENTS = """
SELECT DISTINCT ON (appointment_oid)
    created_at      AS create_dt,
    appointment_oid AS appointment_id,
    source
FROM
    sgm_schedule.appointment_record
WHERE
    created_at >= %s
    AND created_at <  %s
ORDER BY
    appointment_oid,
    updated_at DESC
"""

# Д-3: повторная загрузка исправляет ранее записанное.
INSERT_ONLINE_APPOINTMENTS = """
INSERT INTO kpi.online_appointments (create_dt, appointment_id, "source")
VALUES %s
ON CONFLICT (appointment_id)
DO UPDATE SET
    create_dt = EXCLUDED.create_dt,
    "source"  = EXCLUDED."source"
"""

# Правка 15: сверка объёма с источником
COUNT_IN_ESB = """
SELECT count(DISTINCT appointment_oid)
FROM sgm_schedule.appointment_record
WHERE created_at >= %s
  AND created_at <  %s
"""

COUNT_IN_DWH = """
SELECT count(*)
FROM kpi.online_appointments
WHERE create_dt >= %s
  AND create_dt <  %s
"""

COUNT_BY_SOURCE_DWH = """
SELECT "source", count(*)
FROM kpi.online_appointments
WHERE create_dt >= %s
  AND create_dt <  %s
GROUP BY 1
ORDER BY 1
"""


# ──────────────────────────────────────────────────────────────────────────────
# Работа с датами
# ──────────────────────────────────────────────────────────────────────────────


def add_months(dt: datetime, n: int) -> datetime:
    """
    Д-1: сдвигает на n месяцев, приводя к первому числу.

    Старая версия делала dt.replace(month=dt.month + 1) и падала, когда день
    не существует в следующем месяце: 31 августа → 31 сентября → ValueError.
    Поскольку стартом была максимальная дата в таблице, попадание на 29–31
    число случалось регулярно, и загрузка умирала на несколько дней подряд.
    """
    total = (dt.year * 12 + dt.month - 1) + n
    return dt.replace(
        year=total // 12,
        month=total % 12 + 1,
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )


def utc_now() -> datetime:
    """Д-5: работаем в UTC явно, без наивных datetime."""
    return datetime.now(tz=dt_timezone.utc)


def month_floor(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def last_complete_month_bounds() -> tuple[datetime, datetime]:
    """Границы последнего завершённого месяца, полуинтервал."""
    current_month = month_floor(utc_now())
    return add_months(current_month, -1), current_month


def as_utc(value) -> datetime | None:
    """Приводит значение из базы к UTC-datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt_timezone.utc)
        return value.astimezone(dt_timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=dt_timezone.utc)
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Загрузка
# ──────────────────────────────────────────────────────────────────────────────


def get_last_online_appointment_date() -> datetime | None:
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    result = hook.get_first(GET_LAST_ONLINE_APPOINTMENT)
    value = as_utc(result[0]) if result else None
    logging.info(f"Последняя дата в kpi.online_appointments: {value}")
    return value


def resolve_start(context: dict) -> datetime:
    """
    Д-4: старт считается от текущей даты, а не от максимума в таблице.

    Обычный запуск — скользящее окно RELOAD_DAYS назад.
    Отставание DAG — от последней записи, чтобы догнать пропущенное.
    Пустая таблица — с DATA_FLOOR.
    Ручной бэкфилл — {"backfill_from": "2025-01-01"} в конфигурации запуска.
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    if conf.get("backfill_from"):
        start = datetime.fromisoformat(conf["backfill_from"]).replace(
            tzinfo=dt_timezone.utc
        )
        logging.info(f"Ручной бэкфилл с {start}")
        return max(DATA_FLOOR, start)

    max_date = get_last_online_appointment_date()
    if max_date is None:
        logging.info(f"Таблица пуста, стартуем с {DATA_FLOOR}")
        return DATA_FLOOR

    window_start = utc_now() - timedelta(days=RELOAD_DAYS)
    start = max(DATA_FLOOR, min(window_start, max_date))
    logging.info(
        f"Последняя запись {max_date}, окно с {window_start}, старт {start}"
    )
    return start


def get_online_appointments(start_date: datetime, end_date: datetime) -> list[tuple]:
    hook = PostgresHook(postgres_conn_id="esb_db")
    result = hook.get_records(
        GET_ONLINE_APPOINTMENTS, parameters=(start_date, end_date)
    )
    logging.info(f"ESB {start_date} – {end_date}: {len(result)} записей")
    return result


def load_online_appointments(data: list[tuple]):
    if not data:
        logging.info("Нечего загружать.")
        return
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    conn = hook.get_conn()
    cursor = conn.cursor()
    execute_values(cursor, INSERT_ONLINE_APPOINTMENTS, data, page_size=10000)
    conn.commit()
    cursor.close()
    logging.info(f"Записано {len(data)} строк в kpi.online_appointments")


def insert_online_appointments(**context):
    """
    Грузит записи месячными кусками от resolve_start() до текущего момента.
    Вставка идемпотентна, поэтому повторный проход по уже загруженному
    периоду безопасен и исправляет ранее записанное.
    """
    start_date = resolve_start(context)
    now = utc_now()

    if start_date >= now:
        logging.info(f"Старт {start_date} не раньше текущего момента {now}, пропуск.")
        return

    logging.info(f"Загрузка с {start_date} по {now}")

    total = 0
    first_chunk = True

    while start_date < now:
        # Д-1: следующий кусок — начало следующего месяца, без арифметики по дню
        end_date = min(add_months(month_floor(start_date), 1), now)

        if not first_chunk:
            time.sleep(CHUNK_PAUSE_SECONDS)
        first_chunk = False

        try:
            records = get_online_appointments(start_date, end_date)
            load_online_appointments(records)
            total += len(records)
        except Exception:
            logging.exception(f"Сбой на периоде {start_date} – {end_date}")
            raise

        start_date = end_date

    logging.info(f"Загрузка завершена, обработано {total} записей.")


# ──────────────────────────────────────────────────────────────────────────────
# Правка 15 — сверка с источником
# ──────────────────────────────────────────────────────────────────────────────


def reconcile_with_esb(**context):
    """
    Сравнивает число записей за последний завершённый месяц в ESB и в
    хранилище. Расхождение больше RECONCILE_TOLERANCE означает, что данные
    доехали не полностью.

    Именно эта проверка поймала бы дыры: в августе 2026 в ESB было 10 690
    записей ONLINE_BOOKING, а до витрины дошло 708.
    """
    start, end = last_complete_month_bounds()

    esb_hook = PostgresHook(postgres_conn_id="esb_db")
    dwh_hook = PostgresHook(postgres_conn_id="dwh_pg")

    esb_count = esb_hook.get_first(COUNT_IN_ESB, parameters=(start, end))[0] or 0
    dwh_count = dwh_hook.get_first(COUNT_IN_DWH, parameters=(start, end))[0] or 0

    by_source = dwh_hook.get_records(COUNT_BY_SOURCE_DWH, parameters=(start, end))
    breakdown = ", ".join(f"{src}: {cnt}" for src, cnt in by_source) or "пусто"

    logging.info(
        f"Сверка за {start.date()} – {end.date()}: "
        f"ESB {esb_count}, хранилище {dwh_count}. Разрез: {breakdown}"
    )

    if esb_count == 0:
        raise AirflowException(
            f"В ESB нет записей за {start.date()} – {end.date()}. "
            f"Проверьте доступность sgm_schedule.appointment_record."
        )

    gap = 1 - (dwh_count / esb_count)
    if gap > RECONCILE_TOLERANCE:
        raise AirflowException(
            f"Расхождение с ESB за {start.date()} – {end.date()}: "
            f"в источнике {esb_count}, в хранилище {dwh_count} "
            f"(потеряно {gap:.1%}, допустимо {RECONCILE_TOLERANCE:.0%}). "
            f"Разрез по источникам: {breakdown}. "
            f"Запустите DAG с конфигурацией "
            f'{{"backfill_from": "{start.date()}"}}.'
        )


def check_source_not_null(**context):
    """
    Записи с пустым source отбрасываются при сборке kpi.rate_kpi
    (WHERE oa.source IS NOT NULL), поэтому их доля должна быть известна.
    """
    start, end = last_complete_month_bounds()
    hook = PostgresHook(postgres_conn_id="dwh_pg")

    total, empty = hook.get_first(
        """
        SELECT
            count(*),
            count(*) FILTER (WHERE "source" IS NULL)
        FROM kpi.online_appointments
        WHERE create_dt >= %s AND create_dt < %s
        """,
        parameters=(start, end),
    )

    if not total:
        raise AirflowException(
            f"Нет записей в kpi.online_appointments за {start.date()} – {end.date()}."
        )

    share = empty / total
    logging.info(
        f"Записей за {start.date()} – {end.date()}: {total}, "
        f"из них без source: {empty} ({share:.1%})"
    )

    if share > 0.05:
        raise AirflowException(
            f"У {share:.1%} записей за {start.date()} – {end.date()} не определён "
            f"source. Эти записи выпадают из метрики доли онлайн-записей."
        )


# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "levchenko-an",
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

check_args = {
    "owner": "levchenko-an",
    "retries": 0,
}

with DAG(
    dag_id="patientnet_online_appointments_etl",
    start_date=datetime(2026, 7, 27),
    schedule="0 1 * * *",
    catchup=False,
    default_args=default_args,
    tags=["kpi", "patientnet", "online_appointments"],
    doc_md=__doc__,
) as dag:
    insert_online_appointments_task = PythonOperator(
        task_id="insert_online_appointments",
        python_callable=insert_online_appointments,
    )

    reconcile_task = PythonOperator(
        task_id="reconcile_with_esb",
        python_callable=reconcile_with_esb,
        default_args=check_args,
    )

    check_source_task = PythonOperator(
        task_id="check_source_not_null",
        python_callable=check_source_not_null,
        default_args=check_args,
    )

    insert_online_appointments_task >> reconcile_task >> check_source_task