import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import config
from pathlib import Path


def run_etl():
    """
    Основная функция ETL: выгружает данные из внешней БД и загружает в локальную
    """
    try:
        # Получаем абсолютный путь к папке с DAG
        DAG_DIR = Path(__file__).parent
        sql_path = DAG_DIR / "sql" / "no_show_script.sql"

        print(f"Ищем SQL файл по пути: {sql_path}")

        # 1. Выгружаем из внешней БД
        connection_ext = psycopg2.connect(**config.PN02_DB_CONFIG)

        with open(sql_path, "r", encoding="utf-8") as f:
            sql_query = f.read()

        df = pd.read_sql(sql_query, connection_ext)
        connection_ext.close()
        print(f"Выгружено {len(df)} строк")

        # 2. Записываем в локальную БД
        local_connection_string = f"postgresql+psycopg2://{config.LOCAL_DB_CONFIG['user']}:{config.LOCAL_DB_CONFIG['password']}@{config.LOCAL_DB_CONFIG['host']}:{config.LOCAL_DB_CONFIG['port']}/{config.LOCAL_DB_CONFIG['database']}"

        engine = create_engine(local_connection_string)

        df.to_sql(
            config.TARGET_TABLE_NAME,
            engine,
            schema=config.TARGET_SCHEMA,
            if_exists="replace",
            index=False,
        )

        engine.dispose()
        print(
            f"Успешно записано {len(df)} строк в таблицу {config.TARGET_SCHEMA}.{config.TARGET_TABLE_NAME}"
        )

        return True

    except Exception as e:
        print(f"Ошибка в ETL процессе: {e}")
        return False
