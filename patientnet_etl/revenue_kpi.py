import logging

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import date, datetime, timedelta
from psycopg2.extras import execute_values


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
VALUES ('{month}', {revenue})
ON CONFLICT (month) 
DO UPDATE SET 
    revenue = EXCLUDED.revenue
"""

SELECT_REVENUE_KPI = """
WITH
  base_services AS (
    SELECT
      people.oid AS people_oid,
      services.pay_type,
      presc.id AS presc_id,
      ps.oid AS presc_service_oid
    FROM
      mir.people people
      JOIN mir.mdoc mdoc ON people.oid = mdoc.people_id
      JOIN mir.presc presc ON mdoc.id = presc.mdoc_id
      JOIN mir.presc_service ps ON ps.presc = presc.id
      JOIN mir.service_presctype sp ON ps.service_presctype = sp.oid
      JOIN mir.services services ON sp.service = services.oid
    WHERE
      presc.presc_state_id IN ('sign', 'done_lab', 'done', 'done_other_lpu')
      AND presc.upd_dt >= '{start_month}'
      AND presc.upd_dt <  '{end_month}'
      AND services.pay_type NOT IN ('sp_budget', 'budget')
      AND EXISTS (
        SELECT
          1
        FROM
          mir.sotr sotr
          JOIN mir.sysuser sysuser ON sotr.sysuser = sysuser.oid
        WHERE
          sotr.oid = presc.creator_id
          AND sysuser.oid = '5e95e526-907f-4eef-9093-ac0524a39f5b'
      )
  ),
  finance_plans AS (
    SELECT DISTINCT
      fpps.presc_service AS presc_service_oid
    FROM
      pay.finance_plan_presc_service fpps
      JOIN pay.finance_plan plan ON plan.oid = fpps.finance_plan
      JOIN mir.visit vis ON vis.id = CAST(plan.visit AS bpchar (36))
    WHERE
      vis.pay_type_id = 'cash'
      AND plan.fixed = TRUE
      AND CURRENT_DATE BETWEEN plan.date_begin AND plan.date_end
  ),
  discount_agg AS (
    SELECT
      dp.presc_service AS presc_service_oid,
      SUM(COALESCE(dp.percent, d.percent)) AS sum_percent,
      SUM(dp.fix_sum) AS fix_sum_discount
    FROM
      mir.discount_presc dp
      LEFT JOIN mir.discount d ON d.oid = dp.discount
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
      LEFT JOIN discount_agg da ON da.presc_service_oid = bs.presc_service_oid
  ),
  prices AS (
    SELECT
      bs.presc_service_oid,
      mir.get_price_by_presc (bs.presc_id, bs.presc_service_oid) AS base_price
    FROM
      base_services bs
  ),
  round_setting AS (
    SELECT
      valuepar
    FROM
      mir.systemsettings
    WHERE
      param = 'DiscountAmountRounding'
  )
SELECT
  SUM(
    CASE
      WHEN d.sum_percent IS NOT NULL THEN CASE rs.valuepar
        WHEN '1' THEN CEILING(
          p.base_price - (p.base_price * d.sum_percent / 100) - COALESCE(d.fix_sum_discount, 0)
        )
        WHEN '2' THEN TRUNC(
          p.base_price - (p.base_price * d.sum_percent / 100) - COALESCE(d.fix_sum_discount, 0)
        )
        WHEN '3' THEN ROUND(
          p.base_price - (p.base_price * d.sum_percent / 100) - COALESCE(d.fix_sum_discount, 0)
        )
        ELSE ROUND(
          p.base_price - (p.base_price * d.sum_percent / 100) - COALESCE(d.fix_sum_discount, 0),
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

GET_CALL_CENTER_APPOINTMENTS = """
WITH
  call_center AS (
    SELECT
      s2."oid" AS sotr_oid
    FROM
      mir.sotr s2
      JOIN mir.post p ON p."oid" = s2.post
    WHERE
      p."oid" IN (
        '26a45943-7d97-4034-8405-d00aa050dd57',
        '48064f8f-1ac0-4b30-89f4-a1e93fde29c6'
      )
  )
SELECT
  COUNT(*) AS count
FROM
  mir.schedule s
  JOIN mir.presc_schedule ps ON s.oid = ps.shedule_id
  JOIN mir.presc p ON ps.presc_id = p.id
  JOIN mir.presctype p2 ON ps.presctype = p2."oid"
  JOIN mir.schedule_work_time sw ON s.work_time = sw.oid
WHERE
  s.islocked = 0
  AND ps.presc_id IS NOT NULL
  AND date (p.create_dt) >= '{start_date}'
  AND date (p.create_dt) < '{end_date}'
  AND p.creator_id IN (
    SELECT
      sotr_oid
    FROM
      call_center
  )
  AND p.presctype_id IN (
    SELECT DISTINCT
      (UNNEST(STRING_TO_ARRAY(sw.presctype, ',')))
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
      sys.oid = '5e95e526-907f-4eef-9093-ac0524a39f5b'
  )
SELECT
  p.create_dt,
  p.id
FROM
  mir.schedule s
  JOIN mir.presc_schedule ps ON s.oid = ps.shedule_id
  JOIN mir.presc p ON ps.presc_id = p.id
  JOIN mir.presctype p2 ON ps.presctype = p2."oid"
  JOIN mir.schedule_work_time sw ON s.work_time = sw.oid
WHERE
  ps.presc_id IS NOT NULL
  AND p.creator_id IN (
    SELECT
      oid
    FROM
      cte
  )
  AND date (p.create_dt) >= '{start_date}'
  AND date (p.create_dt) < '{end_date}'
  order by 2 asc
"""

UPDATE_CALL_CENTER_KPI = """
INSERT INTO kpi.rate_kpi (month, source, count)
VALUES ('{create_dt}', 'call_center', {count})
ON CONFLICT (month, source)
DO UPDATE SET
    count = EXCLUDED.count
"""


def add_month(date: date) -> date:
    if date.month == 12:
        result = date.replace(year=date.year + 1, month=1, day=1)
    else:
        result = date.replace(month=date.month + 1, day=1)
    return result


def month_ago(date: date) -> date:
    if date.month == 1:
        result = date.replace(year=date.year - 1, month=12, day=1)
    else:
        result = date.replace(month=date.month - 1, day=1)
    return result


def get_current_month_start() -> date:
    """Возвращает первый день текущего месяца (без времени)."""
    today = date.today()
    return today.replace(day=1)


def get_max_date_of_revenue_kpi():
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    sql = GET_LAST_MONTH
    result = hook.get_first(sql)
    logging.info(f"Last month:, {result}")
    max_date = result[0] if result else None
    return max_date


def select_revenue(start_date: date, end_date: date) -> float:
    hook = PostgresHook(postgres_conn_id="pn_pg")
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    sql = SELECT_REVENUE_KPI.format(start_month=start_str, end_month=end_str)
    result = hook.get_first(sql)
    revenue = result[0] if result and result[0] is not None else 0.0
    logging.info(f"Revenue for period {start_str} – {end_str}: {revenue}")
    return revenue


def update_revenue(month_date: date, revenue: float):
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    month_str = month_date.isoformat()
    sql = UPDATE_REVENUE_KPI.format(month=month_str, revenue=revenue)
    hook.run(sql)
    logging.info(f"Updated revenue for {month_str}: {revenue}")


def get_kpi_revenue(max_date):
    if max_date is None:
        start_date = date(2025, 1, 1)
        logging.info("No previous data found. Starting from 2025-01-01")
    else:
        start_date = month_ago(max_date)
        if start_date < date(2025, 1, 1):
            start_date = date(2025, 1, 1)
        logging.info(f"Last month found: {max_date}. Starting from {start_date}")

    current_month = get_current_month_start()

    while start_date <= current_month:
        end_date = add_month(start_date)
        revenue = select_revenue(start_date, end_date)
        update_revenue(start_date, revenue)
        start_date = end_date

    logging.info("All months up to current have been processed.")


def get_call_center_max_date():
    """
    Получаем последний месяц, за который был выполнен расчет количества записей, сделанных через КЦ.
    TO DO: Возможно, стоит объединить с get_max_date_of_revenue_kpi, так как логика очень похожа.
    """
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    sql = SELECT_LAST_CALL_CENTER_DATE
    result = hook.get_first(sql)
    logging.info(f"Last call center date: {result}")
    logging.info(type(result[0]))
    max_date = result[0] if result else None
    return max_date


def update_call_center_kpi(create_dt: date, count: int):
    """
    Обновляем количество записей, сделанных через КЦ, в таблице kpi
    TO DO: Возможно, стоит объединить с update_revenue, так как логика очень похожа.
    """
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    create_dt_str = create_dt.isoformat()
    sql = UPDATE_CALL_CENTER_KPI.format(create_dt=create_dt_str, count=count)
    hook.run(sql)
    logging.info(f"Updated call center KPI for {create_dt_str}: {count}")


def get_call_center_appointments(start_date: date, end_date: date) -> int:
    """
    Получаем количество записей, сделанных через КЦ, за указанный период.
    TO DO: Возможно, стоит объединить с select_revenue, так как логика очень похожа.
    """
    hook = PostgresHook(postgres_conn_id="pn_pg")
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    sql = GET_CALL_CENTER_APPOINTMENTS.format(start_date=start_str, end_date=end_str)
    result = hook.get_first(sql)
    count = result[0] if result and result[0] is not None else 0
    logging.info(f"Call center appointments from {start_str} to {end_str}: {count}")
    return count


def process_call_center_kpi(max_date):
    """
    Обрабатываем количество записей, сделанных через КЦ, начиная с последнего месяца, за который был выполнен расчет.
    TO DO: Возможно, стоит объединить с get_kpi_revenue, так как логика очень похожа.
    """
    if max_date is None:
        start_date = date(2025, 1, 1)
        logging.info("No previous call center data found. Starting from 2025-01-01")
    else:
        start_date = month_ago(max_date)
        if start_date < date(2025, 1, 1):
            start_date = date(2025, 1, 1)
        logging.info(
            f"Last call center date found: {max_date}. Starting from {start_date}"
        )

    current_month = get_current_month_start()

    while start_date <= current_month:
        end_date = add_month(start_date)
        count = get_call_center_appointments(start_date, end_date)
        update_call_center_kpi(start_date, count)
        start_date = end_date

    logging.info("All call center months up to current have been processed.")


def get_ob_max_date():
    """
    Получаем последнюю дату, за которую у нас есть записи, сделанные через онлайн-канал.
    TO DO: Возможно, стоит объединить с get_max_date_of_revenue_kpi, так как логика очень похожа.
    """
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    sql = SELECT_LAST_OB_DATE
    result = hook.get_first(sql)
    logging.info(f"Last online appointments date: {result}")
    max_date = result[0] if result else None
    return max_date


def load_appointments(data: list[tuple]):
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    conn = hook.get_conn()
    cursor = conn.cursor()
    sql = """
        INSERT INTO kpi.appointments (create_dt, appointment_id)
        VALUES %s
        ON CONFLICT (appointment_id) DO NOTHING
    """
    execute_values(cursor, sql, data, page_size=10000)
    inserted = cursor.rowcount
    conn.commit()
    cursor.close()
    logging.info(f"Inserted {inserted} new records into kpi.appointments")


def get_ob_appointments(start_date: date, end_date: date) -> list[tuple]:
    """
    Получаем все записи, сделанные через онлайн-канал, за указанный период.
    TO DO: Возможно, стоит объединить с get_call_center_appointments, так как логика очень похожа.
    """
    hook = PostgresHook(postgres_conn_id="pn_pg")
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    sql = GET_OB_APPOINTMENTS.format(start_date=start_str, end_date=end_str)
    result = hook.get_records(sql)
    logging.info(
        f"Online appointments from {start_str} to {end_str}: {len(result)} records found"
    )
    return result


def process_ob_appointments(max_date):
    """
    Обрабатываем все записи, сделанные через онлайн-канал, начиная с последнего месяца, за который был выполнен расчет.
    TO DO: Возможно, стоит объединить с process_call_center_kpi, так как логика очень похожа.
    """
    if max_date is None:
        start_date = date(2025, 1, 1)
        logging.info(
            "No previous online appointments data found. Starting from 2025-01-01"
        )
    else:
        start_date = month_ago(max_date)
        if start_date < date(2025, 1, 1):
            start_date = date(2025, 1, 1)
        logging.info(
            f"Last online appointments date found: {max_date}. Starting from {start_date}"
        )

    current_month = get_current_month_start()

    while start_date <= current_month:
        end_date = add_month(start_date)
        appointments = get_ob_appointments(start_date, end_date)
        load_appointments(appointments)
        start_date = end_date

    logging.info("All online appointments months up to current have been processed.")


default_args = {
    "owner": "levchenko-an",
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="patientnet_etl",
    start_date=datetime(2026, 7, 27),
    schedule="0 1 * * *",
    catchup=False,
    default_args=default_args,
    tags=["kpi", "patientnet", "revenue"],
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

    (
        get_last_month
        >> get_revenue
        >> get_call_center_last_month
        >> process_call_center_amount
        >> get_ob_last_month
        >> process_ob_appointments_
    )
