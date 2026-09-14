import calendar
import logging
import os
from datetime import date, datetime, timedelta

import requests
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, timezone
from airflow.utils.trigger_rule import TriggerRule

# ──────────────────────────────────────────────────────────────────────────────
# Константы
# ──────────────────────────────────────────────────────────────────────────────

# Глубина скользящего пересчёта витрин MAU в месяцах, включая текущий.
RECALC_MONTHS = 3

# Сколько дней назад проверяем источники на суточные пропуски
GAP_WINDOW_DAYS = 30

# Падение объёма относительно медианы, при котором считаем день или месяц битым
VOLUME_DROP_THRESHOLD = 0.4

# Сдвиг доли пользователей без ehr_id, при котором падаем (процентные пункты)
COVERAGE_SHIFT_PP = 20

# Сколько дней берём в базу для медианы доли покрытия
COVERAGE_BASELINE_DAYS = 14


def load_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_yaml(filename):
    return load_file(
        os.path.join(os.path.dirname(__file__), "yaml_templates", filename)
    )


def load_sql(filename):
    return load_file(os.path.join(os.path.dirname(__file__), "sql", filename))


# ──────────────────────────────────────────────────────────────────────────────
# Работа с датами
# ──────────────────────────────────────────────────────────────────────────────


def get_current_month_start() -> date:
    return timezone.utcnow().date().replace(day=1)


def last_complete_month() -> date:
    """
    Первый день последнего ЗАВЕРШЁННОГО месяца.
    Правка 3: всё, что уходит в годовые итоги, обрезается по эту границу.
    """
    return get_current_month_start() - relativedelta(months=1)


def recalc_window_start() -> date:
    """Правка 14: начало скользящего окна пересборки витрин."""
    return get_current_month_start() - relativedelta(months=RECALC_MONTHS - 1)


# ──────────────────────────────────────────────────────────────────────────────
# Загрузка источников
# ──────────────────────────────────────────────────────────────────────────────


def load_source(table_name, date_field, yaml_template, source_name, **context):
    """
    ВНИМАНИЕ. Здесь заложен механизм, из-за которого появляются дыры:
    старт загрузки берётся как max(date_field) + 1 день. Если день загрузился
    частично, max уже равен этому дню, и он больше никогда не догрузится.

    Пока не выяснено, идемпотентен ли transformer при повторной загрузке
    того же дня, перезалить автоматически нельзя — иначе есть риск задвоить
    события. Поэтому дыры сейчас ЛОВЯТСЯ (check_source_gaps), а закрываются
    вручную. См. otvety-po-pravkam-2.md, раздел про идемпотентность.
    """
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


# ──────────────────────────────────────────────────────────────────────────────
# SQL
# ──────────────────────────────────────────────────────────────────────────────

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

MAU_OB_SQL = """
INSERT INTO kpi.mau_ob_kpi
(
    month,
    count
)
SELECT
    %(month)s::date,
    COUNT(
        DISTINCT COALESCE(
            internal_user_id::text,
            source || ':' || external_user_id
        )
    )
FROM kpi.user_presence
WHERE
    product='oz'
AND activity_date >= %(month)s::date
AND activity_date < %(next_month)s::date

ON CONFLICT(month)
DO UPDATE
SET count=excluded.count;
"""

MAU_LK_SQL = """
INSERT INTO kpi.mau_lk_kpi(month,count)
WITH lk_users AS (
    SELECT
        COALESCE(
            internal_user_id::text,
            external_user_id
        ) AS user_id
    FROM kpi.user_presence
    WHERE
        product='lk'
    AND platform='mobile'
    AND activity_date >= %(month)s::date
    AND activity_date < %(next_month)s::date
    UNION
    SELECT
        internal_user_id::text
    FROM kpi.user_presence
    WHERE
        product='lk'
    AND platform='web'
    AND internal_user_id IS NOT NULL
    AND activity_date >= %(month)s::date
    AND activity_date < %(next_month)s::date

)
SELECT
    %(month)s::date,
    COUNT(DISTINCT user_id)
FROM lk_users
ON CONFLICT(month)
DO UPDATE
SET count=excluded.count;
"""

# правка 23 — полуинтервал вместо BETWEEN
TOTAL_OB_USERS_SQL = """
INSERT INTO kpi.kpis_summary
(
    year,
    kpi,
    value
)
SELECT
    %(year)s,
    'mau_ob',
    COUNT(
        DISTINCT COALESCE(
            internal_user_id::text,
            source || ':' || external_user_id
        )
    )
FROM kpi.user_presence
WHERE product='oz'
  AND activity_date >= %(start_date)s
  AND activity_date <  %(end_date)s
ON CONFLICT (year, kpi)
DO UPDATE SET
    value = EXCLUDED.value;
"""

# правка 3 — среднее MAU ЛК только по завершённым месяцам текущего года
MAU_LK_AVG_SQL = """
INSERT INTO kpi.kpis_summary
(
    year,
    kpi,
    value
)
SELECT
    %(year)s,
    'mau_lk_avg',
    COALESCE(AVG(count), 0)
FROM kpi.mau_lk_kpi
WHERE month >= %(start_month)s::date
  AND month <  %(end_month)s::date
ON CONFLICT (year, kpi)
DO UPDATE SET
    value = EXCLUDED.value;
"""

# правка 15 — суточные пропуски в источнике
CHECK_SOURCE_GAPS_SQL = """
WITH calendar AS (
    SELECT generate_series(
        %(end_day)s::date - %(window)s::int,
        %(end_day)s::date,
        INTERVAL '1 day'
    )::date AS d
),
daily AS (
    SELECT DATE({date_field}) AS d, count(*) AS cnt
    FROM {table_name}
    WHERE {date_field} >= (%(end_day)s::date - %(window)s::int)
      AND {date_field} <  (%(end_day)s::date + INTERVAL '1 day')
    GROUP BY 1
),
joined AS (
    SELECT c.d, COALESCE(x.cnt, 0) AS cnt
    FROM calendar c
    LEFT JOIN daily x ON x.d = c.d
)
SELECT
    j.d,
    j.cnt,
    (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY cnt)
     FROM joined WHERE cnt > 0) AS median_cnt
FROM joined j
ORDER BY j.d
"""

# правка 15 — помесячный объём user_presence по каждой связке
CHECK_PRESENCE_VOLUME_SQL = """
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', activity_date)::date AS month,
        source,
        product,
        platform,
        count(*) AS cnt
    FROM kpi.user_presence
    WHERE activity_date >= (%(month)s::date - INTERVAL '6 months')
      AND activity_date <  (%(month)s::date + INTERVAL '1 month')
    GROUP BY 1, 2, 3, 4
)
SELECT
    source,
    product,
    platform,
    COALESCE(MAX(CASE WHEN month = %(month)s::date THEN cnt END), 0)
        AS current_cnt,
    COALESCE(MAX(CASE WHEN month = (%(month)s::date - INTERVAL '1 month')::date
                      THEN cnt END), 0)
        AS prev_cnt,
    COALESCE(percentile_cont(0.5) WITHIN GROUP (
        ORDER BY CASE WHEN month < %(month)s::date THEN cnt END
    ), 0) AS median_prev
FROM monthly
GROUP BY source, product, platform
ORDER BY source, product, platform
"""

# правка 16 — покрытие идентификаторами
UPSERT_IDENTITY_COVERAGE_SQL = """
INSERT INTO kpi.identity_coverage
    (activity_date, source, product, platform, rows_total, rows_no_ehr, share_no_ehr)
SELECT
    activity_date,
    source,
    product,
    platform,
    count(*)                                                        AS rows_total,
    count(*) FILTER (WHERE internal_user_id IS NULL)                AS rows_no_ehr,
    round(100.0 * count(*) FILTER (WHERE internal_user_id IS NULL)
          / NULLIF(count(*), 0), 2)                                 AS share_no_ehr
FROM kpi.user_presence
WHERE activity_date >= %(start_date)s::date
  AND activity_date <  (%(end_date)s::date + INTERVAL '1 day')
GROUP BY 1, 2, 3, 4
ON CONFLICT (activity_date, source, product, platform)
DO UPDATE SET
    rows_total   = EXCLUDED.rows_total,
    rows_no_ehr  = EXCLUDED.rows_no_ehr,
    share_no_ehr = EXCLUDED.share_no_ehr,
    updated_at   = now();
"""

# правка 17 — резкий сдвиг доли без ehr_id
CHECK_IDENTITY_COVERAGE_SQL = """
WITH latest AS (
    SELECT source, product, platform, share_no_ehr
    FROM kpi.identity_coverage
    WHERE activity_date = %(day)s::date
),
baseline AS (
    SELECT
        source, product, platform,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY share_no_ehr) AS median_share
    FROM kpi.identity_coverage
    WHERE activity_date >= %(day)s::date - %(window)s::int
      AND activity_date <  %(day)s::date
    GROUP BY source, product, platform
)
SELECT
    l.source, l.product, l.platform, l.share_no_ehr, b.median_share
FROM latest l
JOIN baseline b USING (source, product, platform)
WHERE abs(l.share_no_ehr - b.median_share) > %(shift)s
"""

# правка 18 — витрины MAU не должны быть пустыми за завершённые месяцы
CHECK_MAU_SANITY_SQL = """
SELECT
    count(*) FILTER (WHERE count IS NULL OR count <= 0) AS bad_months,
    count(*)                                            AS total_months
FROM {table_name}
WHERE month >= %(start_month)s::date
  AND month <  %(end_month)s::date
"""


# ──────────────────────────────────────────────────────────────────────────────
# Исполнение
# ──────────────────────────────────────────────────────────────────────────────


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


def execute_presence(sql_template, sql_params, source_task, **context):
    ti = context["ti"]
    dates = ti.xcom_pull(task_ids=source_task)
    if not dates:
        raise Exception(f"No dates from {source_task}")

    format_params = {**sql_params, **dates}
    formatted_sql = sql_template.format(**format_params)

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


# ──────────────────────────────────────────────────────────────────────────────
# Правка 15 — контроль полноты
# ──────────────────────────────────────────────────────────────────────────────


def check_source_gaps(table_name, date_field, source_name, **context):
    """
    Ищет дни с нулевым или аномально низким числом событий за последние
    GAP_WINDOW_DAYS дней. Именно так выглядят дыры за июль 2025 и август 2026.
    """
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    end_day = timezone.utcnow().date() - timedelta(days=1)

    sql = CHECK_SOURCE_GAPS_SQL.format(
        table_name=table_name,
        date_field=date_field,
    )
    rows = hook.get_records(
        sql, parameters={"end_day": end_day, "window": GAP_WINDOW_DAYS}
    )

    if not rows:
        raise AirflowException(f"{source_name}: нет данных за последние {GAP_WINDOW_DAYS} дней.")

    median_cnt = float(rows[0][2] or 0)
    if median_cnt <= 0:
        logging.warning(f"{source_name}: нет базы для сравнения, проверка пропущена.")
        return

    floor = median_cnt * (1 - VOLUME_DROP_THRESHOLD)
    gaps = [(d, cnt) for d, cnt, _ in rows if cnt < floor]

    logging.info(
        f"{source_name}: медиана {median_cnt:.0f} событий в день, "
        f"порог {floor:.0f}, проблемных дней {len(gaps)}"
    )

    if gaps:
        details = ", ".join(f"{d}: {cnt}" for d, cnt in gaps[:10])
        raise AirflowException(
            f"{source_name}: обнаружены пропуски в загрузке. "
            f"Дни с объёмом ниже {floor:.0f} событий: {details}"
            + (f" и ещё {len(gaps) - 10}" if len(gaps) > 10 else "")
            + ". Данные за эти дни нужно перезалить вручную."
        )


def check_presence_volume(**context):
    """
    Помесячный объём user_presence по каждой связке источник/продукт/платформа.

    Источники, выведенные из эксплуатации, пропускаются: если в проверяемом
    месяце ноль И в предыдущем ноль — значит источник больше не пишет,
    а не сломался. Так ведёт себя Amplitude, переставший писать 01.02.2026.
    Сломанная загрузка выглядит иначе: падение с ненулевого значения.
    """
    month = last_complete_month()
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    rows = hook.get_records(CHECK_PRESENCE_VOLUME_SQL, parameters={"month": month})

    problems = []
    for source, product, platform, current_cnt, prev_cnt, median_prev in rows:
        median_prev = float(median_prev or 0)
        label = f"{source}/{product}/{platform}"

        if current_cnt == 0 and prev_cnt == 0:
            logging.info(f"{label}: источник не активен, проверка пропущена")
            continue

        logging.info(
            f"{label} за {month}: {current_cnt}, медиана {median_prev:.0f}"
        )

        if median_prev <= 0:
            continue

        drop = 1 - (current_cnt / median_prev)
        if drop > VOLUME_DROP_THRESHOLD:
            problems.append(
                f"{label}: {current_cnt} против медианы {median_prev:.0f} "
                f"(падение {drop:.0%})"
            )

    if problems:
        raise AirflowException(
            f"Неполный user_presence за {month}: " + "; ".join(problems)
        )


# ──────────────────────────────────────────────────────────────────────────────
# Правки 16 и 17 — покрытие идентификаторами
# ──────────────────────────────────────────────────────────────────────────────


def build_identity_coverage(source_task, **context):
    """Правка 16: ежедневная доля пользователей без ehr_id по каждой связке."""
    ti = context["ti"]
    dates = ti.xcom_pull(task_ids=source_task)
    if not dates:
        # Источник скипнулся — считаем покрытие за вчера
        yesterday = (timezone.utcnow().date() - timedelta(days=1)).isoformat()
        dates = {"start_date": yesterday, "end_date": yesterday}

    hook = PostgresHook(postgres_conn_id="dwh_pg")
    hook.run(
        UPSERT_IDENTITY_COVERAGE_SQL,
        parameters={
            "start_date": dates["start_date"],
            "end_date": dates["end_date"],
        },
    )
    logging.info(
        f"Покрытие идентификаторами обновлено за "
        f"{dates['start_date']} – {dates['end_date']}"
    )


def check_identity_coverage(**context):
    """
    Правка 17: если доля пользователей без ehr_id резко изменилась, вероятнее
    всего сломалась kpi.safe_extract_user_id — например, Яндекс.Метрика
    изменила структуру параметров, и функция молча возвращает NULL.
    """
    day = timezone.utcnow().date() - timedelta(days=1)
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    rows = hook.get_records(
        CHECK_IDENTITY_COVERAGE_SQL,
        parameters={
            "day": day,
            "window": COVERAGE_BASELINE_DAYS,
            "shift": COVERAGE_SHIFT_PP,
        },
    )

    if rows:
        details = "; ".join(
            f"{src}/{prod}/{plat}: {share}% против медианы {median}%"
            for src, prod, plat, share, median in rows
        )
        raise AirflowException(
            f"Доля пользователей без ehr_id за {day} сдвинулась больше чем на "
            f"{COVERAGE_SHIFT_PP} п.п.: {details}. Вероятная причина — изменился "
            f"формат параметров Яндекс.Метрики и kpi.safe_extract_user_id "
            f"молча возвращает NULL."
        )

    logging.info(f"Покрытие идентификаторами за {day} в норме.")


# ──────────────────────────────────────────────────────────────────────────────
# Витрины
# ──────────────────────────────────────────────────────────────────────────────


def rebuild_monthly_kpi(table_name, start_if_empty, sql_template, **context):
    """
    Правка 14: окно пересборки отсчитывается от текущей даты, а не от
    MAX(month). Раньше месяц, выпавший из окна, замораживался навсегда.

    Ручной бэкфилл: запуск DAG с конфигурацией
        {"backfill_from": "2025-01-01"}
    """
    hook = PostgresHook(postgres_conn_id="dwh_pg")

    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    last_month = hook.get_first(f"SELECT MAX(month) FROM {table_name}")[0]

    if conf.get("backfill_from"):
        start_month = date.fromisoformat(conf["backfill_from"])
        logging.info(f"{table_name}: ручной бэкфилл с {start_month}")
    elif last_month is None:
        start_month = start_if_empty
        logging.info(f"{table_name}: данных нет, стартуем с {start_month}")
    else:
        window = recalc_window_start()
        start_month = max(start_if_empty, min(window, last_month.replace(day=1)))
        logging.info(
            f"{table_name}: последний месяц {last_month}, окно с {window}, "
            f"старт {start_month}"
        )

    current_month = get_current_month_start()
    month = start_month

    while month <= current_month:
        next_month = month + relativedelta(months=1)
        logging.info(f"Rebuild {table_name}: {month} - {next_month}")
        hook.run(
            sql_template,
            parameters={"month": month, "next_month": next_month},
        )
        month = next_month


def rebuild_total_ob_users(**context):
    """
    Годовой охват онлайн-записи, сопоставимые YTD-периоды.

    Правка 23: полуинтервал вместо BETWEEN.
    Правка 3:  верхняя граница — вчерашний день, а не сегодняшний. Сегодня
               данные ещё не загружены, и текущий год терял бы один день
               против прошлого.
    """
    hook = PostgresHook(postgres_conn_id="dwh_pg")

    today = timezone.utcnow().date()
    yesterday = today - timedelta(days=1)
    current_year = yesterday.year

    for year in range(2025, current_year + 1):
        start_date = date(year, 1, 1)

        if year == current_year:
            end_exclusive = yesterday + timedelta(days=1)
        else:
            try:
                anchor = date(year, yesterday.month, yesterday.day)
            except ValueError:
                anchor = date(
                    year,
                    yesterday.month,
                    calendar.monthrange(year, yesterday.month)[1],
                )
            end_exclusive = anchor + timedelta(days=1)

        logging.info(
            f"Rebuild total_ob_users за {year}: {start_date} – {end_exclusive} (искл.)"
        )

        hook.run(
            TOTAL_OB_USERS_SQL,
            parameters={
                "year": year,
                "start_date": start_date,
                "end_date": end_exclusive,
            },
        )


def rebuild_mau_lk_avg(**context):
    """
    Правка 3: среднегодовой MAU ЛК считается ТОЛЬКО по завершённым месяцам.

    Раньше среднее считалось в Metabase по всем строкам mau_lk_kpi, включая
    текущий неполный месяц. На 8 сентября это давало 46 498 вместо 48 380 —
    занижение почти на 1 900 пользователей при цели 45 000.
    """
    hook = PostgresHook(postgres_conn_id="dwh_pg")

    end_month = get_current_month_start()
    year = end_month.year if end_month.month > 1 else end_month.year - 1
    start_month = date(year, 1, 1)

    if start_month >= end_month:
        logging.warning("Нет завершённых месяцев в текущем году, расчёт пропущен.")
        return

    hook.run(
        MAU_LK_AVG_SQL,
        parameters={
            "year": year,
            "start_month": start_month,
            "end_month": end_month,
        },
    )

    value = hook.get_first(
        "SELECT value FROM kpi.kpis_summary WHERE year = %s AND kpi = 'mau_lk_avg'",
        parameters=(year,),
    )
    logging.info(
        f"Средний MAU ЛК за {start_month} – {end_month} (искл.): "
        f"{value[0] if value else None}"
    )


def check_mau_sanity(**context):
    """Правка 18: за завершённые месяцы витрины MAU не должны быть пустыми."""
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    start_month = recalc_window_start()
    end_month = get_current_month_start()

    if start_month >= end_month:
        logging.warning("Нет завершённых месяцев в окне, проверка пропущена.")
        return

    problems = []
    for table in ("kpi.mau_ob_kpi", "kpi.mau_lk_kpi"):
        bad, total = hook.get_first(
            CHECK_MAU_SANITY_SQL.format(table_name=table),
            parameters={"start_month": start_month, "end_month": end_month},
        )
        logging.info(f"{table}: месяцев {total}, из них пустых {bad}")
        if total == 0:
            problems.append(f"{table}: нет ни одного месяца в окне")
        elif bad:
            problems.append(f"{table}: {bad} месяцев с нулевым значением")

    if problems:
        raise AirflowException(
            f"Проблемы в витринах MAU за {start_month} – {end_month} (искл.): "
            + "; ".join(problems)
        )


# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "levchenko-an",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Загрузка ходит по сети: сетевую ошибку имеет смысл повторить, иначе день
# не загрузится, а завтрашний прогон его уже не подхватит.
load_args = {
    "owner": "levchenko-an",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

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
    doc_md=__doc__,
) as dag:
    # ==========================================================
    # Источники данных
    # ==========================================================

    load_appmetrica = PythonOperator(
        task_id="load_appmetrica",
        python_callable=load_source,
        default_args=load_args,
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
        default_args=load_args,
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
        default_args=load_args,
        op_kwargs={
            "table_name": "yandex_metrika_web_lk.events",
            "date_field": "date_time",
            "yaml_template": load_yaml("web_lk.yaml"),
            "source_name": "Web LK",
        },
    )

    rebuild_mau_ob = PythonOperator(
        task_id="rebuild_mau_ob",
        python_callable=rebuild_monthly_kpi,
        op_kwargs={
            "table_name": "kpi.mau_ob_kpi",
            "start_if_empty": datetime(2025, 1, 1).date(),
            "sql_template": MAU_OB_SQL,
        },
    )

    rebuild_mau_lk = PythonOperator(
        task_id="rebuild_mau_lk",
        python_callable=rebuild_monthly_kpi,
        op_kwargs={
            "table_name": "kpi.mau_lk_kpi",
            "start_if_empty": datetime(2026, 1, 1).date(),
            "sql_template": MAU_LK_SQL,
        },
    )

    rebuild_total_ob_users_task = PythonOperator(
        task_id="rebuild_total_ob_users",
        python_callable=rebuild_total_ob_users,
    )

    rebuild_mau_lk_avg_task = PythonOperator(
        task_id="rebuild_mau_lk_avg",
        python_callable=rebuild_mau_lk_avg,
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
            "sql_params": {
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
        },
        # ---------- AppMetrica LK ----------
        # Осознанное решение: присутствием в ЛК считается любое событие
        # приложения, включая dashboard_open (вход на главную ЛК).
        {
            "task_id": "presence_appmetrica_lk",
            "source_task": "load_appmetrica",
            "sql_params": {
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
        },
        # ---------- Booking ----------
        {
            "task_id": "presence_booking",
            "source_task": "load_booking",
            "sql_params": {
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
        },
        # ---------- Web LK ----------
        {
            "task_id": "presence_web_lk",
            "source_task": "load_web_lk",
            "sql_params": {
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
        },
    ]

    presence_tasks = {}

    for cfg in presence_configs:
        presence_tasks[cfg["task_id"]] = PythonOperator(
            task_id=cfg["task_id"],
            python_callable=execute_presence,
            op_kwargs={
                "source_task": cfg["source_task"],
                "sql_template": PRESENCE_SQL,
                "sql_params": cfg["sql_params"],
            },
        )

    # ==========================================================
    # Правка 15 — контроль пропусков в источниках
    # ==========================================================

    gap_configs = [
        ("check_gaps_appmetrica", "appmetrica.events", "event_datetime", "AppMetrica"),
        ("check_gaps_booking", "yandex_metrika_booking.events", "date_time", "Booking"),
        ("check_gaps_web_lk", "yandex_metrika_web_lk.events", "date_time", "Web LK"),
    ]

    gap_tasks = {}
    for task_id, table_name, date_field, source_name in gap_configs:
        gap_tasks[task_id] = PythonOperator(
            task_id=task_id,
            python_callable=check_source_gaps,
            trigger_rule=TriggerRule.ALL_DONE,
            op_kwargs={
                "table_name": table_name,
                "date_field": date_field,
                "source_name": source_name,
            },
        )

    # ==========================================================
    # Правки 16–18 — покрытие и качество
    # ==========================================================

    coverage_tasks = {}
    for task_id, source_task in [
        ("coverage_appmetrica", "load_appmetrica"),
        ("coverage_booking", "load_booking"),
        ("coverage_web_lk", "load_web_lk"),
    ]:
        coverage_tasks[task_id] = PythonOperator(
            task_id=task_id,
            python_callable=build_identity_coverage,
            trigger_rule=TriggerRule.ALL_DONE,
            op_kwargs={"source_task": source_task},
        )

    check_coverage_task = PythonOperator(
        task_id="check_identity_coverage",
        python_callable=check_identity_coverage,
    )

    check_presence_volume_task = PythonOperator(
        task_id="check_presence_volume",
        python_callable=check_presence_volume,
    )

    check_mau_sanity_task = PythonOperator(
        task_id="check_mau_sanity",
        python_callable=check_mau_sanity,
    )

    start_booking = PythonOperator(
        task_id="start_booking",
        python_callable=lambda: None,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start_web_lk = PythonOperator(
        task_id="start_web_lk",
        python_callable=lambda: None,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    start_kpi = PythonOperator(
        task_id="start_kpi",
        python_callable=lambda: None,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # ===================
    # Scheme
    # ===================
    #
    #    AppMetrica
    #        │
    #    Identity
    #        │
    # ┌───────┴────────┐
    # │                │
    # Presence OZ   Presence LK
    # └──────┬─────────┘
    #        │
    #    Coverage + Gaps
    #        │
    #    ALL_DONE
    #        │
    #    Booking   → Identity → Presence → Coverage + Gaps
    #        │
    #    Web LK    → Identity → Presence → Coverage + Gaps
    #        │
    #   NONE_FAILED
    #        │
    #   check_presence_volume
    #        │
    #   check_identity_coverage
    #        │
    #    MAU OB → MAU LK → check_mau_sanity
    #        │
    #   Total OB → MAU LK avg

    # ==========================================================
    # AppMetrica
    # ==========================================================

    load_appmetrica >> identity_tasks["build_identity_appmetrica"]

    identity_tasks["build_identity_appmetrica"] >> [
        presence_tasks["presence_appmetrica_oz"],
        presence_tasks["presence_appmetrica_lk"],
    ]

    [
        presence_tasks["presence_appmetrica_oz"],
        presence_tasks["presence_appmetrica_lk"],
    ] >> coverage_tasks["coverage_appmetrica"] >> gap_tasks["check_gaps_appmetrica"]

    # ==========================================================
    # Booking
    # ==========================================================
    gap_tasks["check_gaps_appmetrica"] >> start_booking

    start_booking >> load_booking

    (
        load_booking
        >> identity_tasks["build_identity_booking"]
        >> presence_tasks["presence_booking"]
        >> coverage_tasks["coverage_booking"]
        >> gap_tasks["check_gaps_booking"]
    )

    # ==========================================================
    # Web LK
    # ==========================================================
    gap_tasks["check_gaps_booking"] >> start_web_lk

    start_web_lk >> load_web_lk

    (
        load_web_lk
        >> identity_tasks["build_identity_web_lk"]
        >> presence_tasks["presence_web_lk"]
        >> coverage_tasks["coverage_web_lk"]
        >> gap_tasks["check_gaps_web_lk"]
    )

    # ==========================================================
    # KPI
    # ==========================================================
    [
        gap_tasks["check_gaps_booking"],
        gap_tasks["check_gaps_web_lk"],
    ] >> start_kpi

    (
        start_kpi
        >> check_presence_volume_task
        >> check_coverage_task
        >> rebuild_mau_ob
        >> rebuild_mau_lk
        >> check_mau_sanity_task
        >> rebuild_total_ob_users_task
        >> rebuild_mau_lk_avg_task
    )