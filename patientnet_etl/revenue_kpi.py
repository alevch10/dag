import logging
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from psycopg2.extras import execute_values

# ──────────────────────────────────────────────────────────────────────────────
# Константы
# ──────────────────────────────────────────────────────────────────────────────

# Глубина скользящего пересчёта в месяцах, включая текущий.
RECALC_MONTHS = 3

# Раньше какой даты данных нет
DATA_FLOOR = date(2025, 1, 1)

# Порог падения помесячного объёма относительно медианы
VOLUME_DROP_THRESHOLD = 0.4

# Сколько месяцев берём для медианы при контроле объёма
VOLUME_BASELINE_MONTHS = 6

# Источник записей колл-центра в rate_kpi
CALL_CENTER_SOURCE = "call_center"


def get_self_booking_sysuser() -> str:
    """UUID учётной записи «Самозапись» в МИС."""
    return Variable.get(
        "kpi_self_booking_sysuser",
        default_var="5e95e526-907f-4eef-9093-ac0524a39f5b",
    )


def get_call_center_posts() -> list[str]:
    """UUID должностей, относящихся к колл-центру."""
    return Variable.get(
        "kpi_call_center_posts",
        deserialize_json=True,
        default_var=[
            "26a45943-7d97-4034-8405-d00aa050dd57",
            "48064f8f-1ac0-4b30-89f4-a1e93fde29c6",
        ],
    )


def get_web_online_sources() -> list[str]:
    """
    Источники ESB, которые считаются веб-онлайн-записью — числитель метрики.

    Мобильные источники (MOBILE_LK, MOBILE_ONLINE_BOOKING) в метрику НЕ входят:
    приложение удаляли из сторов, и повлиять на этот канал команда не могла.
    Это осознанное решение, а не пропуск.
    """
    return Variable.get(
        "kpi_web_online_sources",
        deserialize_json=True,
        default_var=["ONLINE_BOOKING", "LK"],
    )


def get_known_sources() -> list[str]:
    """
    Все источники, которые мы ожидаем увидеть в rate_kpi.
    Появление нового означает, что ESB начал размечать канал по-новому,
    и метрику надо пересматривать, а не молча его игнорировать.
    """
    return Variable.get(
        "kpi_known_sources",
        deserialize_json=True,
        default_var=[
            "ONLINE_BOOKING",
            "LK",
            "MOBILE_LK",
            "MOBILE_ONLINE_BOOKING",
            "EXTERNAL",
            "call_center",
        ],
    )


def to_sql_list(values: list[str]) -> str:
    """Превращает список строк в 'a', 'b' для подстановки в IN (...)."""
    safe = [v.replace("'", "") for v in values]
    return ", ".join(f"'{v}'" for v in safe)


# ──────────────────────────────────────────────────────────────────────────────
# SQL
# ──────────────────────────────────────────────────────────────────────────────

GET_LAST_MONTH = """
SELECT
    month::date
FROM
    kpi.revenue_kpi
ORDER BY
    month DESC
LIMIT 1
"""

UPDATE_REVENUE_KPI = """
INSERT INTO kpi.revenue_kpi (month, revenue)
VALUES (%(month)s, %(revenue)s)
ON CONFLICT (month)
DO UPDATE SET
    revenue = EXCLUDED.revenue
"""

# правка 7  — round_setting возвращает ровно одну строку всегда
# правка 8  — ветка со скидкой срабатывает и при только фиксированной сумме
# правка 10 — финплан проверяется на конец отчётного периода
# правка 19 — sysuser подставляется из переменной
# правка 20 — discount_agg ограничен услугами периода
SELECT_REVENUE_KPI = """
WITH
  base_services AS (
    SELECT
      presc.id AS presc_id,
      ps.oid   AS presc_service_oid
    FROM
      mir.people people
      JOIN mir.mdoc mdoc            ON people.oid = mdoc.people_id
      JOIN mir.presc presc          ON mdoc.id = presc.mdoc_id
      JOIN mir.presc_service ps     ON ps.presc = presc.id
      JOIN mir.service_presctype sp ON ps.service_presctype = sp.oid
      JOIN mir.services services    ON sp.service = services.oid
    WHERE
      presc.presc_state_id IN ('sign', 'done_lab', 'done', 'done_other_lpu')
      AND presc.upd_dt >= '{start_month}'
      AND presc.upd_dt <  '{end_month}'
      AND services.pay_type NOT IN ('sp_budget', 'budget')
      AND EXISTS (
        SELECT 1
        FROM
          mir.sotr sotr
          JOIN mir.sysuser sysuser ON sotr.sysuser = sysuser.oid
        WHERE
          sotr.oid = presc.creator_id
          AND sysuser.oid = '{self_booking_sysuser}'
      )
  ),
  finance_plans AS (
    SELECT DISTINCT
      fpps.presc_service AS presc_service_oid
    FROM
      pay.finance_plan_presc_service fpps
      JOIN pay.finance_plan plan ON plan.oid = fpps.finance_plan
      JOIN mir.visit vis         ON vis.id = CAST(plan.visit AS bpchar (36))
    WHERE
      vis.pay_type_id = 'cash'
      AND plan.fixed = TRUE
      AND ('{end_month}'::date - INTERVAL '1 day')
          BETWEEN plan.date_begin AND plan.date_end
  ),
  discount_agg AS (
    SELECT
      dp.presc_service AS presc_service_oid,
      SUM(COALESCE(dp.percent, d.percent)) AS sum_percent,
      SUM(dp.fix_sum)                      AS fix_sum_discount
    FROM
      mir.discount_presc dp
      LEFT JOIN mir.discount d ON d.oid = dp.discount
    WHERE
      dp.presc_service IN (SELECT presc_service_oid FROM base_services)
    GROUP BY
      dp.presc_service
  ),
  discounts AS (
    SELECT
      bs.presc_service_oid,
      CASE
        WHEN fp.presc_service_oid IS NULL THEN da.sum_percent
      END AS sum_percent,
      CASE
        WHEN fp.presc_service_oid IS NULL THEN da.fix_sum_discount
      END AS fix_sum_discount
    FROM
      base_services bs
      LEFT JOIN finance_plans fp ON fp.presc_service_oid = bs.presc_service_oid
      LEFT JOIN discount_agg  da ON da.presc_service_oid = bs.presc_service_oid
  ),
  prices AS (
    SELECT
      bs.presc_service_oid,
      mir.get_price_by_presc (bs.presc_id, bs.presc_service_oid) AS base_price
    FROM
      base_services bs
  ),
  round_setting AS (
    SELECT COALESCE(
      (
        SELECT valuepar
        FROM mir.systemsettings
        WHERE param = 'DiscountAmountRounding'
        LIMIT 1
      ),
      ''
    ) AS valuepar
  )
SELECT
  SUM(
    CASE
      WHEN d.sum_percent IS NOT NULL OR d.fix_sum_discount IS NOT NULL THEN
        CASE rs.valuepar
          WHEN '1' THEN CEILING(
            p.base_price
            - (p.base_price * COALESCE(d.sum_percent, 0) / 100)
            - COALESCE(d.fix_sum_discount, 0)
          )
          WHEN '2' THEN TRUNC(
            p.base_price
            - (p.base_price * COALESCE(d.sum_percent, 0) / 100)
            - COALESCE(d.fix_sum_discount, 0)
          )
          WHEN '3' THEN ROUND(
            p.base_price
            - (p.base_price * COALESCE(d.sum_percent, 0) / 100)
            - COALESCE(d.fix_sum_discount, 0)
          )
          ELSE ROUND(
            p.base_price
            - (p.base_price * COALESCE(d.sum_percent, 0) / 100)
            - COALESCE(d.fix_sum_discount, 0),
            2
          )
        END
      ELSE p.base_price
    END
  ) AS revenue
FROM
  prices p
  JOIN discounts d ON d.presc_service_oid = p.presc_service_oid
  CROSS JOIN round_setting rs;
"""

SELECT_LAST_CALL_CENTER_DATE = """
SELECT
    month::date
FROM
    kpi.rate_kpi
WHERE source = 'call_center'
ORDER BY
    month DESC
LIMIT 1
"""

SELECT_LAST_OB_DATE = """
SELECT
    DATE_TRUNC('month', create_dt)::date AS month
FROM
    kpi.appointments
ORDER BY
    month DESC
LIMIT 1
"""

# правка 4  — принадлежность к колл-центру берётся на дату создания записи
# правка 19 — должности подставляются из переменной
# правка 23 — границы периода полуинтервалом по timestamp, без date()
GET_CALL_CENTER_APPOINTMENTS = """
WITH
  call_center AS (
    SELECT
      s2."oid" AS sotr_oid,
      COALESCE(s2.date_post_begin, '2000-01-01'::timestamp) AS post_begin,
      COALESCE(s2.date_post_end,   '2999-12-31'::timestamp) AS post_end
    FROM
      mir.sotr s2
      JOIN mir.post p ON p."oid" = s2.post
    WHERE
      p."oid" IN ({call_center_posts})
  )
SELECT
  COUNT(DISTINCT p.id) AS count
FROM
  mir.schedule s
  JOIN mir.presc_schedule ps     ON s.oid = ps.shedule_id
  JOIN mir.presc p               ON ps.presc_id = p.id
  JOIN mir.presctype p2          ON ps.presctype = p2."oid"
  JOIN mir.schedule_work_time sw ON s.work_time = sw.oid
  JOIN call_center cc            ON cc.sotr_oid = p.creator_id
                                AND p.create_dt >= cc.post_begin
                                AND p.create_dt <  cc.post_end
WHERE
  s.islocked = 0
  AND ps.presc_id IS NOT NULL
  AND p.create_dt >= '{start_date}'
  AND p.create_dt <  '{end_date}'
  AND p.presctype_id IN (
    SELECT DISTINCT (UNNEST(STRING_TO_ARRAY(sw.presctype, ',')))
  )
  AND p2.time_cells IS NOT NULL
  AND (
    'CHILD'::VARCHAR = ANY (p2.tags)
    OR 'ADULT'::VARCHAR = ANY (p2.tags)
  )
  AND sw.insite = 1
"""

GET_OB_APPOINTMENTS = """
WITH
  cte AS (
    SELECT
      so.oid AS oid
    FROM
      mir.sotr so
      JOIN mir.sysuser sys ON so.sysuser = sys.oid
    WHERE
      sys.oid = '{self_booking_sysuser}'
  )
SELECT DISTINCT ON (p.id)
  p.create_dt,
  p.id
FROM
  mir.schedule s
  JOIN mir.presc_schedule ps     ON s.oid = ps.shedule_id
  JOIN mir.presc p               ON ps.presc_id = p.id
  JOIN mir.presctype p2          ON ps.presctype = p2."oid"
  JOIN mir.schedule_work_time sw ON s.work_time = sw.oid
WHERE
  ps.presc_id IS NOT NULL
  AND p.creator_id IN (SELECT oid FROM cte)
  AND p.create_dt >= '{start_date}'
  AND p.create_dt <  '{end_date}'
ORDER BY
  p.id
"""

UPDATE_CALL_CENTER_KPI = """
INSERT INTO kpi.rate_kpi (month, source, count)
VALUES (%(month)s, 'call_center', %(count)s)
ON CONFLICT (month, source)
DO UPDATE SET
    count = EXCLUDED.count
"""

# правка 12 — повторный прогон исправляет уже загруженное
LOAD_APPOINTMENTS = """
INSERT INTO kpi.appointments (create_dt, appointment_id)
VALUES %s
ON CONFLICT (appointment_id)
DO UPDATE SET
    create_dt = EXCLUDED.create_dt
"""

# ── НОВОЕ В ВЕРСИИ 2 ──────────────────────────────────────────────────────────

# Сборка rate_kpi по источникам из ESB.
# Отличия от запроса, который выполнялся вручную:
#   - ограничен окном пересчёта, а не пересобирает всю историю каждый раз;
#   - COUNT(DISTINCT) вместо COUNT(*) как защита от дублей.
REBUILD_RATE_KPI = """
INSERT INTO kpi.rate_kpi (month, source, count)
SELECT
    DATE_TRUNC('month', a.create_dt)::date AS month,
    oa."source",
    COUNT(DISTINCT a.appointment_id)       AS count
FROM
    kpi.appointments a
    JOIN kpi.online_appointments oa ON a.appointment_id = oa.appointment_id
WHERE
    oa."source" IS NOT NULL
    AND a.create_dt >= %(start_month)s::date
    AND a.create_dt <  %(end_month)s::date
GROUP BY 1, 2
ON CONFLICT (month, source)
DO UPDATE SET
    count = EXCLUDED.count
"""

# Витрина доли считается ИЗ rate_kpi, а не из отдельных источников.
# Благодаря этому числитель и знаменатель физически не могут разойтись
# между таблицами — раньше расхождение достигало десяти раз.
REBUILD_OB_TO_CALL_CENTRE = """
INSERT INTO kpi.ob_to_call_centre_kpi
    (month, online_count, online_and_callcenter, ratio_online_to_total)
SELECT
    r.month,
    COALESCE(SUM(r.count) FILTER (WHERE r.source IN ({web_sources})), 0)
        AS online_count,
    COALESCE(SUM(r.count) FILTER (
        WHERE r.source IN ({web_sources}) OR r.source = '{call_center_source}'
    ), 0) AS online_and_callcenter,
    COALESCE(SUM(r.count) FILTER (WHERE r.source IN ({web_sources})), 0)::numeric
        / NULLIF(COALESCE(SUM(r.count) FILTER (
            WHERE r.source IN ({web_sources}) OR r.source = '{call_center_source}'
          ), 0), 0) AS ratio_online_to_total
FROM
    kpi.rate_kpi r
WHERE
    r.month >= %(start_month)s::date
    AND r.month <  %(end_month)s::date
GROUP BY
    r.month
HAVING
    COALESCE(SUM(r.count) FILTER (
        WHERE r.source IN ({web_sources}) OR r.source = '{call_center_source}'
    ), 0) > 0
ON CONFLICT (month)
DO UPDATE SET
    online_count          = EXCLUDED.online_count,
    online_and_callcenter = EXCLUDED.online_and_callcenter,
    ratio_online_to_total = EXCLUDED.ratio_online_to_total
"""

# Новый источник в ESB не должен молча пройти мимо метрики
CHECK_KNOWN_SOURCES = """
SELECT DISTINCT source
FROM kpi.rate_kpi
WHERE month >= %(start_month)s::date
  AND month <  %(end_month)s::date
  AND source NOT IN ({known_sources})
ORDER BY source
"""

# ──────────────────────────────────────────────────────────────────────────────

UPDATE_KPIS_SUMMARY = """
INSERT INTO kpi.kpis_summary (year, kpi, value)
VALUES (%s, %s, %s)
ON CONFLICT (year, kpi)
DO UPDATE SET
    value = EXCLUDED.value
"""

# правка 23 — полуинтервал
SUM_REVENUE_PERIOD = """
SELECT COALESCE(SUM(revenue), 0)
FROM kpi.revenue_kpi
WHERE month >= %s AND month < %s
"""

AVG_RATIO_PERIOD = """
SELECT COALESCE(AVG(ratio_online_to_total), 0)
FROM kpi.ob_to_call_centre_kpi
WHERE month >= %s AND month < %s
"""

# правка 15 — контроль полноты помесячной загрузки
CHECK_APPOINTMENTS_VOLUME = """
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', create_dt)::date AS month,
        count(*)                             AS cnt
    FROM kpi.appointments
    WHERE create_dt >= (%(month)s::date - INTERVAL '%(baseline)s months')
      AND create_dt <  (%(month)s::date + INTERVAL '1 month')
    GROUP BY 1
)
SELECT
    COALESCE((SELECT cnt FROM monthly WHERE month = %(month)s::date), 0) AS current_cnt,
    COALESCE((
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY cnt)
        FROM monthly
        WHERE month < %(month)s::date
    ), 0) AS median_prev
"""

CHECK_RATE_KPI_VOLUME = """
WITH monthly AS (
    SELECT month, source, count
    FROM kpi.rate_kpi
    WHERE month >= (%(month)s::date - INTERVAL '%(baseline)s months')
      AND month <= %(month)s::date
)
SELECT
    m.source,
    COALESCE(MAX(CASE WHEN m.month = %(month)s::date THEN m.count END), 0) AS current_cnt,
    COALESCE(percentile_cont(0.5) WITHIN GROUP (
        ORDER BY CASE WHEN m.month < %(month)s::date THEN m.count END
    ), 0) AS median_prev
FROM monthly m
GROUP BY m.source
ORDER BY m.source
"""

# правка 18 — проверка диапазона доли
CHECK_RATIO_RANGE = """
SELECT
    count(*) FILTER (WHERE ratio_online_to_total IS NULL)                  AS null_rows,
    count(*) FILTER (WHERE ratio_online_to_total < 0
                        OR ratio_online_to_total > 1)                      AS out_of_range,
    count(*) FILTER (WHERE online_and_callcenter IS NULL
                        OR online_and_callcenter <= 0)                     AS empty_denominator,
    count(*)                                                               AS total_rows
FROM kpi.ob_to_call_centre_kpi
WHERE month >= %s AND month < %s
"""


# ──────────────────────────────────────────────────────────────────────────────
# Работа с датами
# ──────────────────────────────────────────────────────────────────────────────


def add_months(d: date, n: int) -> date:
    """Сдвигает дату на n месяцев, возвращая первое число."""
    total = (d.year * 12 + d.month - 1) + n
    return date(total // 12, total % 12 + 1, 1)


def add_month(d: date) -> date:
    return add_months(d, 1)


def month_ago(d: date) -> date:
    return add_months(d, -1)


def get_current_month_start() -> date:
    """Первый день текущего месяца."""
    return date.today().replace(day=1)


def last_complete_month() -> date:
    """
    Первый день последнего ЗАВЕРШЁННОГО месяца.
    Правка 3: всё, что сравнивается год к году, обрезается по эту границу.
    """
    return month_ago(get_current_month_start())


def resolve_start_month(max_date: date | None, context: dict) -> date:
    """
    Правка 2: с какого месяца начинать пересчёт.

    Обычный запуск   — скользящее окно RECALC_MONTHS месяцев назад.
    Отставание DAG   — от последнего записанного месяца, чтобы догнать.
    Первый запуск    — с DATA_FLOOR.
    Ручной бэкфилл   — дата из dag_run.conf: {"backfill_from": "2025-01-01"}
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    if conf.get("backfill_from"):
        start = date.fromisoformat(conf["backfill_from"])
        logging.info(f"Ручной бэкфилл с {start}")
        return max(DATA_FLOOR, start)

    if max_date is None:
        logging.info(f"Данных нет, стартуем с {DATA_FLOOR}")
        return DATA_FLOOR

    window_start = add_months(get_current_month_start(), -(RECALC_MONTHS - 1))
    start = max(DATA_FLOOR, min(window_start, max_date))
    logging.info(
        f"Последний записанный месяц {max_date}, окно с {window_start}, старт {start}"
    )
    return start


def resolve_rebuild_window(context: dict) -> tuple[date, date]:
    """
    Окно пересборки витрин rate_kpi и ob_to_call_centre_kpi.
    Возвращает (начало, конец исключительно).
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    if conf.get("backfill_from"):
        start = max(DATA_FLOOR, date.fromisoformat(conf["backfill_from"]))
    else:
        start = max(
            DATA_FLOOR,
            add_months(get_current_month_start(), -(RECALC_MONTHS - 1)),
        )

    end = add_month(get_current_month_start())
    logging.info(f"Окно пересборки витрин: {start} – {end} (исключительно)")
    return start, end


# ──────────────────────────────────────────────────────────────────────────────
# Выручка
# ──────────────────────────────────────────────────────────────────────────────


def get_max_date_of_revenue_kpi():
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    result = hook.get_first(GET_LAST_MONTH)
    max_date = result[0] if result else None
    logging.info(f"Последний месяц в revenue_kpi: {max_date}")
    return max_date


def select_revenue(start_date: date, end_date: date) -> float:
    hook = PostgresHook(postgres_conn_id="pn_pg")
    sql = SELECT_REVENUE_KPI.format(
        start_month=start_date.isoformat(),
        end_month=end_date.isoformat(),
        self_booking_sysuser=get_self_booking_sysuser(),
    )
    result = hook.get_first(sql)
    revenue = result[0] if result and result[0] is not None else 0.0
    logging.info(f"Выручка за {start_date} – {end_date}: {revenue}")
    return revenue


def update_revenue(month_date: date, revenue: float):
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    hook.run(UPDATE_REVENUE_KPI, parameters={"month": month_date, "revenue": revenue})
    logging.info(f"Записана выручка за {month_date}: {revenue}")


def get_kpi_revenue(max_date, **context):
    start_date = resolve_start_month(max_date, context)
    current_month = get_current_month_start()
    complete = last_complete_month()

    while start_date <= current_month:
        end_date = add_month(start_date)
        revenue = select_revenue(start_date, end_date)

        if start_date <= complete and (revenue is None or revenue <= 0):
            raise AirflowException(
                f"Выручка за завершённый месяц {start_date} равна {revenue}. "
                f"Проверьте настройку DiscountAmountRounding и доступность "
                f"mir.get_price_by_presc."
            )

        update_revenue(start_date, revenue)
        start_date = end_date

    logging.info("Выручка посчитана по текущий месяц включительно.")


# ──────────────────────────────────────────────────────────────────────────────
# Колл-центр
# ──────────────────────────────────────────────────────────────────────────────


def get_call_center_max_date():
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    result = hook.get_first(SELECT_LAST_CALL_CENTER_DATE)
    max_date = result[0] if result else None
    logging.info(f"Последний месяц колл-центра: {max_date}")
    return max_date


def update_call_center_kpi(month: date, count: int):
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    hook.run(UPDATE_CALL_CENTER_KPI, parameters={"month": month, "count": count})
    logging.info(f"Записаны записи колл-центра за {month}: {count}")


def get_call_center_appointments(start_date: date, end_date: date) -> int:
    hook = PostgresHook(postgres_conn_id="pn_pg")
    sql = GET_CALL_CENTER_APPOINTMENTS.format(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        call_center_posts=to_sql_list(get_call_center_posts()),
    )
    result = hook.get_first(sql)
    count = result[0] if result and result[0] is not None else 0
    logging.info(f"Записи колл-центра за {start_date} – {end_date}: {count}")
    return count


def process_call_center_kpi(max_date, **context):
    start_date = resolve_start_month(max_date, context)
    current_month = get_current_month_start()
    complete = last_complete_month()

    while start_date <= current_month:
        end_date = add_month(start_date)
        count = get_call_center_appointments(start_date, end_date)

        if start_date <= complete and count == 0:
            raise AirflowException(
                f"Ноль записей колл-центра за завершённый месяц {start_date}. "
                f"Проверьте переменную kpi_call_center_posts и заполнение "
                f"date_post_begin / date_post_end в mir.sotr."
            )

        update_call_center_kpi(start_date, count)
        start_date = end_date

    logging.info("Колл-центр посчитан по текущий месяц включительно.")


# ──────────────────────────────────────────────────────────────────────────────
# Записи через онлайн-канал
# ──────────────────────────────────────────────────────────────────────────────


def get_ob_max_date():
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    result = hook.get_first(SELECT_LAST_OB_DATE)
    max_date = result[0] if result else None
    logging.info(f"Последний месяц онлайн-записей: {max_date}")
    return max_date


def load_appointments(data: list[tuple]):
    if not data:
        logging.info("Нечего загружать.")
        return
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    conn = hook.get_conn()
    cursor = conn.cursor()
    execute_values(cursor, LOAD_APPOINTMENTS, data, page_size=10000)
    conn.commit()
    cursor.close()
    logging.info(f"Загружено {len(data)} записей в kpi.appointments")


def get_ob_appointments(start_date: date, end_date: date) -> list[tuple]:
    hook = PostgresHook(postgres_conn_id="pn_pg")
    sql = GET_OB_APPOINTMENTS.format(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        self_booking_sysuser=get_self_booking_sysuser(),
    )
    result = hook.get_records(sql)
    logging.info(f"Онлайн-записи за {start_date} – {end_date}: {len(result)}")
    return result


def process_ob_appointments(max_date, **context):
    start_date = resolve_start_month(max_date, context)
    current_month = get_current_month_start()
    complete = last_complete_month()

    while start_date <= current_month:
        end_date = add_month(start_date)
        appointments = get_ob_appointments(start_date, end_date)

        if start_date <= complete and not appointments:
            raise AirflowException(
                f"Ноль онлайн-записей за завершённый месяц {start_date}. "
                f"Проверьте переменную kpi_self_booking_sysuser."
            )

        load_appointments(appointments)
        start_date = end_date

    logging.info("Онлайн-записи загружены по текущий месяц включительно.")


# ──────────────────────────────────────────────────────────────────────────────
# Витрины доли — новое в версии 2
# ──────────────────────────────────────────────────────────────────────────────


def rebuild_rate_kpi(**context):
    """
    Собирает строки kpi.rate_kpi по источникам из ESB.

    Раньше этот запрос выполнялся вне Airflow и без ограничения периодом.
    Здесь он привязан к окну пересчёта, поэтому стоимость не растёт
    со временем, а строка call_center остаётся нетронутой — её пишет
    отдельный шаг выше.
    """
    start_month, end_month = resolve_rebuild_window(context)

    hook = PostgresHook(postgres_conn_id="dwh_pg")
    hook.run(
        REBUILD_RATE_KPI,
        parameters={"start_month": start_month, "end_month": end_month},
    )

    rows = hook.get_records(
        """
        SELECT month, source, count
        FROM kpi.rate_kpi
        WHERE month >= %(start_month)s::date
          AND month <  %(end_month)s::date
        ORDER BY month, source
        """,
        parameters={"start_month": start_month, "end_month": end_month},
    )
    for month, source, count in rows:
        logging.info(f"rate_kpi {month} / {source}: {count}")


def check_known_sources(**context):
    """
    Если в ESB появился новый источник, он молча не попадёт ни в числитель,
    ни в знаменатель метрики. Так, например, в апреле 2026 появился источник
    LK — запись из веб-личного кабинета, и до его добавления в список
    метрика недосчитывала бы растущий канал.
    """
    start_month, end_month = resolve_rebuild_window(context)

    hook = PostgresHook(postgres_conn_id="dwh_pg")
    sql = CHECK_KNOWN_SOURCES.format(known_sources=to_sql_list(get_known_sources()))
    rows = hook.get_records(
        sql, parameters={"start_month": start_month, "end_month": end_month}
    )

    if rows:
        unknown = ", ".join(r[0] for r in rows)
        raise AirflowException(
            f"В kpi.rate_kpi появились источники, которых нет в списке известных: "
            f"{unknown}. Решите, входят ли они в метрику доли, и добавьте их "
            f"в переменные kpi_known_sources и, если нужно, kpi_web_online_sources."
        )

    logging.info("Новых источников не появилось.")


def rebuild_ob_to_call_centre(**context):
    """
    Считает витрину доли ИЗ kpi.rate_kpi.

    Ключевое отличие от прежней схемы: числитель и знаменатель берутся из
    одной таблицы и одним запросом, поэтому разойтись между собой они больше
    не могут. Раньше знаменатель в витрине за август 2026 отличался от
    rate_kpi в десять раз, и заметить это можно было только вручную.
    """
    start_month, end_month = resolve_rebuild_window(context)

    hook = PostgresHook(postgres_conn_id="dwh_pg")
    sql = REBUILD_OB_TO_CALL_CENTRE.format(
        web_sources=to_sql_list(get_web_online_sources()),
        call_center_source=CALL_CENTER_SOURCE,
    )
    hook.run(
        sql, parameters={"start_month": start_month, "end_month": end_month}
    )

    rows = hook.get_records(
        """
        SELECT month, online_count, online_and_callcenter, ratio_online_to_total
        FROM kpi.ob_to_call_centre_kpi
        WHERE month >= %(start_month)s::date
          AND month <  %(end_month)s::date
        ORDER BY month
        """,
        parameters={"start_month": start_month, "end_month": end_month},
    )
    for month, online, total, ratio in rows:
        logging.info(f"Доля за {month}: {online} / {total} = {ratio}")


# ──────────────────────────────────────────────────────────────────────────────
# Правка 15 — контроль полноты
# ──────────────────────────────────────────────────────────────────────────────


def check_appointments_volume(**context):
    """
    Сравнивает объём последнего завершённого месяца с медианой предыдущих.
    Именно эта проверка поймала бы дыры за июль 2025 и август 2026.
    """
    month = last_complete_month()
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    row = hook.get_first(
        CHECK_APPOINTMENTS_VOLUME,
        parameters={"month": month, "baseline": VOLUME_BASELINE_MONTHS},
    )
    current_cnt, median_prev = row[0], float(row[1] or 0)

    logging.info(
        f"kpi.appointments за {month}: {current_cnt}, "
        f"медиана предыдущих {VOLUME_BASELINE_MONTHS} мес: {median_prev}"
    )

    if median_prev <= 0:
        logging.warning("Нет базы для сравнения, проверка пропущена.")
        return

    drop = 1 - (current_cnt / median_prev)
    if drop > VOLUME_DROP_THRESHOLD:
        raise AirflowException(
            f"Объём kpi.appointments за {month} — {current_cnt}, что на "
            f"{drop:.0%} ниже медианы {median_prev:.0f}. Похоже на неполную "
            f"загрузку. Порог: {VOLUME_DROP_THRESHOLD:.0%}."
        )


def check_rate_kpi_volume(**context):
    """То же самое, но в разрезе источников rate_kpi."""
    month = last_complete_month()
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    rows = hook.get_records(
        CHECK_RATE_KPI_VOLUME,
        parameters={"month": month, "baseline": VOLUME_BASELINE_MONTHS},
    )

    problems = []
    for source, current_cnt, median_prev in rows:
        median_prev = float(median_prev or 0)
        logging.info(f"{source} за {month}: {current_cnt}, медиана {median_prev}")
        if median_prev <= 0:
            continue
        drop = 1 - (current_cnt / median_prev)
        if drop > VOLUME_DROP_THRESHOLD:
            problems.append(
                f"{source}: {current_cnt} против медианы {median_prev:.0f} "
                f"(падение {drop:.0%})"
            )

    if problems:
        raise AirflowException(
            f"Неполная загрузка kpi.rate_kpi за {month}: " + "; ".join(problems)
        )


def check_ratio_range(**context):
    """Правка 18: доля должна лежать в 0–1 и иметь непустой знаменатель."""
    end = add_month(last_complete_month())
    start = add_months(end, -RECALC_MONTHS)
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    null_rows, out_of_range, empty_denominator, total_rows = hook.get_first(
        CHECK_RATIO_RANGE, parameters=(start, end)
    )

    if total_rows == 0:
        raise AirflowException(
            f"В kpi.ob_to_call_centre_kpi нет данных за {start} – {end}."
        )

    if null_rows or out_of_range or empty_denominator:
        raise AirflowException(
            f"Некорректные значения в kpi.ob_to_call_centre_kpi за {start} – {end}: "
            f"пустых {null_rows}, вне диапазона 0–1 {out_of_range}, "
            f"с нулевым знаменателем {empty_denominator}."
        )

    logging.info(f"Доли за {start} – {end}: {total_rows} месяцев, все в норме.")


# ──────────────────────────────────────────────────────────────────────────────
# Итоговые метрики год к году
# ──────────────────────────────────────────────────────────────────────────────


def get_max_month_ratio() -> date | None:
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    result = hook.get_first(
        "SELECT month FROM kpi.ob_to_call_centre_kpi ORDER BY month DESC LIMIT 1"
    )
    max_month = result[0] if result else None
    logging.info(f"Последний месяц в ob_to_call_centre_kpi: {max_month}")
    return max_month


def resolve_yoy_period(max_month: date) -> tuple[date, date, date, date]:
    """
    Правка 3: период YoY обрезается последним ЗАВЕРШЁННЫМ месяцем.
    Правка 23: верхние границы возвращаются исключающими.

    Год берётся из УЖЕ обрезанной даты. Если взять его раньше, в январе
    start_current окажется больше end_current и выборка станет пустой.
    """
    end_current = min(max_month, last_complete_month())
    year = end_current.year

    start_current = date(year, 1, 1)
    start_previous = date(year - 1, 1, 1)
    end_previous = date(year - 1, end_current.month, 1)

    logging.info(
        f"Период YoY: {start_current} – {end_current} против "
        f"{start_previous} – {end_previous}"
    )
    return (
        start_current,
        add_month(end_current),
        start_previous,
        add_month(end_previous),
    )


def calculate_revenue_yoy_diff():
    """Разница выручки текущего года и прошлого за сопоставимые периоды."""
    hook = PostgresHook(postgres_conn_id="dwh_pg")

    max_month = get_max_date_of_revenue_kpi()
    if max_month is None:
        raise AirflowException("В kpi.revenue_kpi нет данных.")

    start_cur, end_cur, start_prev, end_prev = resolve_yoy_period(max_month)

    if start_cur >= end_cur:
        logging.warning(
            "Нет ни одного завершённого месяца в текущем году, расчёт пропущен."
        )
        return

    current_sum = hook.get_first(SUM_REVENUE_PERIOD, parameters=(start_cur, end_cur))[0]
    previous_sum = hook.get_first(SUM_REVENUE_PERIOD, parameters=(start_prev, end_prev))[0]

    diff = current_sum - previous_sum
    logging.info(f"Разница выручки: {diff} (текущий {current_sum}, прошлый {previous_sum})")

    hook.run(UPDATE_KPIS_SUMMARY, parameters=(start_cur.year, "revenue", diff))
    logging.info(f"Записано в kpis_summary: year={start_cur.year}, kpi='revenue', {diff}")


def calculate_ratio_yoy_diff():
    """Разница средних долей онлайн-записи за сопоставимые периоды."""
    hook = PostgresHook(postgres_conn_id="dwh_pg")

    max_month = get_max_month_ratio()
    if max_month is None:
        raise AirflowException("В kpi.ob_to_call_centre_kpi нет данных.")

    start_cur, end_cur, start_prev, end_prev = resolve_yoy_period(max_month)

    if start_cur >= end_cur:
        logging.warning(
            "Нет ни одного завершённого месяца в текущем году, расчёт пропущен."
        )
        return

    current_avg = hook.get_first(AVG_RATIO_PERIOD, parameters=(start_cur, end_cur))[0]
    previous_avg = hook.get_first(AVG_RATIO_PERIOD, parameters=(start_prev, end_prev))[0]

    diff = current_avg - previous_avg
    logging.info(f"Разница доли: {diff} (текущая {current_avg}, прошлая {previous_avg})")

    hook.run(UPDATE_KPIS_SUMMARY, parameters=(start_cur.year, "ratio", diff))
    logging.info(f"Записано в kpis_summary: year={start_cur.year}, kpi='ratio', {diff}")


# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "levchenko-an",
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

# Проверки качества не ретраим: данные за 30 минут сами не починятся,
# ретрай только оттягивает момент, когда станет видно проблему.
check_args = {
    "owner": "levchenko-an",
    "retries": 0,
}

with DAG(
    dag_id="patientnet_etl",
    start_date=datetime(2026, 7, 27),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["kpi", "patientnet", "revenue"],
    doc_md=__doc__,
) as dag:
    get_last_month = PythonOperator(
        task_id="get_last_month",
        python_callable=get_max_date_of_revenue_kpi,
    )
    get_revenue = PythonOperator(
        task_id="get_revenue",
        python_callable=get_kpi_revenue,
        op_args=[get_last_month.output],
    )
    get_call_center_last_month = PythonOperator(
        task_id="get_call_center_last_month",
        python_callable=get_call_center_max_date,
    )
    process_call_center_amount = PythonOperator(
        task_id="process_call_center_amount",
        python_callable=process_call_center_kpi,
        op_args=[get_call_center_last_month.output],
    )
    get_ob_last_month = PythonOperator(
        task_id="get_ob_last_month",
        python_callable=get_ob_max_date,
    )
    process_ob_appointments_ = PythonOperator(
        task_id="process_ob_appointments",
        python_callable=process_ob_appointments,
        op_args=[get_ob_last_month.output],
    )

    check_appointments_volume_task = PythonOperator(
        task_id="check_appointments_volume",
        python_callable=check_appointments_volume,
        default_args=check_args,
    )

    rebuild_rate_kpi_task = PythonOperator(
        task_id="rebuild_rate_kpi",
        python_callable=rebuild_rate_kpi,
    )
    check_known_sources_task = PythonOperator(
        task_id="check_known_sources",
        python_callable=check_known_sources,
        default_args=check_args,
    )
    check_rate_kpi_volume_task = PythonOperator(
        task_id="check_rate_kpi_volume",
        python_callable=check_rate_kpi_volume,
        default_args=check_args,
    )
    rebuild_ob_to_call_centre_task = PythonOperator(
        task_id="rebuild_ob_to_call_centre",
        python_callable=rebuild_ob_to_call_centre,
    )
    check_ratio_range_task = PythonOperator(
        task_id="check_ratio_range",
        python_callable=check_ratio_range,
        default_args=check_args,
    )

    calculate_revenue_diff_task = PythonOperator(
        task_id="calculate_revenue_yoy_diff",
        python_callable=calculate_revenue_yoy_diff,
    )
    calculate_ratio_diff_task = PythonOperator(
        task_id="calculate_ratio_yoy_diff",
        python_callable=calculate_ratio_yoy_diff,
    )

    (
        get_last_month
        >> get_revenue
        >> get_call_center_last_month
        >> process_call_center_amount
        >> get_ob_last_month
        >> process_ob_appointments_
        >> check_appointments_volume_task
        >> rebuild_rate_kpi_task
        >> check_known_sources_task
        >> check_rate_kpi_volume_task
        >> rebuild_ob_to_call_centre_task
        >> check_ratio_range_task
        >> calculate_revenue_diff_task
        >> calculate_ratio_diff_task
    )