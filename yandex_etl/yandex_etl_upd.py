from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import requests
import logging
import os

# ---------- Путь к папке с YAML-шаблонами ----------
YAML_DIR = os.path.join(os.path.dirname(__file__), "yaml_templates")


# ---------- Функция загрузки YAML из файла ----------
def load_yaml_template(filename):
    with open(os.path.join(YAML_DIR, filename), "r") as f:
        return f.read()


# ---------- Переменные из Airflow ----------
DWH_HELPER_URL = Variable.get("dwh_helper_url", default_var="http://dwh-helper:8000")
BEARER_TOKEN = Variable.get("BEARER", default_var="")
HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/x-yaml",
}

# ---------- Загружаем шаблоны ----------
APPMETRICA_YAML = load_yaml_template("appmetrica.yaml")
BOOKING_YAML = load_yaml_template("booking.yaml")
WEBLK_YAML = load_yaml_template("web_lk.yaml")


# ---------- Вспомогательная функция ----------
def get_yaml_with_dates(template, start_date, end_date):
    return template.format(start_date=start_date, end_date=end_date)


# ---------- Основная функция загрузки ----------
def load_data(
    table_name: str, date_field: str, yaml_template: str, source_name: str, **context
):
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    sql = f"SELECT max({date_field}) FROM {table_name}"
    logging.info(f"Выполняем SQL-запрос: {sql}")
    result = hook.get_first(sql)
    logging.info(f"Результат запроса: {result}")
    max_date = result[0] if result and result[0] else None

    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    if max_date is None:
        start_date = yesterday - timedelta(days=7)
        end_date = yesterday
        logging.warning(
            f"Таблица {table_name} пуста, загружаем за последние 7 дней ({start_date} - {end_date})"
        )
    else:
        if isinstance(max_date, datetime):
            max_date = max_date.date()
        if max_date < yesterday:
            start_date = max_date + timedelta(days=1)
            end_date = yesterday
        else:
            logging.info(
                f"Данные за вчера ({yesterday}) уже есть в {table_name}, пропускаем."
            )
            # ✅ Правильно завершаем задачу как пропущенную, а не перепланируем
            raise AirflowSkipException(
                f"Данные за {yesterday} уже загружены, пропускаем."
            )

    yaml_payload = get_yaml_with_dates(
        yaml_template, start_date.isoformat(), end_date.isoformat()
    )

    url = f"{DWH_HELPER_URL}/etl/transformer?start_after_line=0"
    try:
        response = requests.post(url, data=yaml_payload, headers=HEADERS, timeout=72000)
        response.raise_for_status()
        resp_json = response.json()

        # Логируем все поля ответа
        logging.info(f"=== Ответ от API для {source_name} ===")
        logging.info(f"Status: {resp_json.get('status')}")
        logging.info(f"Message: {resp_json.get('message')}")
        if "statistics" in resp_json and resp_json["statistics"]:
            logging.info(f"Statistics: {resp_json['statistics']}")
        if "failed_file" in resp_json and resp_json["failed_file"]:
            logging.warning(
                f"Failed file: {resp_json['failed_file']}, line: {resp_json.get('failed_line')}"
            )
        if "last_successful_file" in resp_json and resp_json["last_successful_file"]:
            logging.info(
                f"Last successful file: {resp_json['last_successful_file']}, line: {resp_json.get('last_successful_line')}"
            )
        if "error_details" in resp_json and resp_json["error_details"]:
            logging.error(f"Error details: {resp_json['error_details']}")

        # Сохраняем ответ в XCom для дальнейшего использования
        ti = context["ti"]
        ti.xcom_push(key=f"{source_name}_response", value=resp_json)

        if resp_json.get("status") == "success":
            logging.info(
                f"Данные для {source_name} успешно загружены за период {start_date} - {end_date}"
            )
        else:
            raise ValueError(
                f"Не успешный статус: {resp_json.get('message', 'Unknown error')}"
            )

    except Exception as e:
        logging.error(f"Ошибка при загрузке {source_name}: {e}")
        raise


# ---------- DAG ----------
default_args = {
    "owner": "levchenko-an",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 11, 10),
    "catchup": False,
}

with DAG(
    dag_id="etl_yandex_appmetrica",
    default_args=default_args,
    schedule="0 3 * * *",
    description="Ежедневная загрузка данных из AppMetrica и Яндекс.Метрики в DWH",
    tags=["etl", "appmetrica", "yandex_metrika"],
) as dag:
    task_appmetrica = PythonOperator(
        task_id="load_appmetrica",
        python_callable=load_data,
        op_kwargs={
            "table_name": "appmetrica.events",
            "date_field": "event_datetime",
            "yaml_template": APPMETRICA_YAML,
            "source_name": "AppMetrica",
        },
    )

    task_booking = PythonOperator(
        task_id="load_yandex_booking",
        python_callable=load_data,
        op_kwargs={
            "table_name": "yandex_metrika_booking.events",
            "date_field": "date_time",
            "yaml_template": BOOKING_YAML,
            "source_name": "Yandex Metrika Booking",
        },
    )

    task_web_lk = PythonOperator(
        task_id="load_yandex_web_lk",
        python_callable=load_data,
        op_kwargs={
            "table_name": "yandex_metrika_web_lk.events",
            "date_field": "date_time",
            "yaml_template": WEBLK_YAML,
            "source_name": "Yandex Metrika Web LK",
        },
    )

    task_appmetrica >> task_booking >> task_web_lk
