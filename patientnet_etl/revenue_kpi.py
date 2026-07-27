import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta


GET_LAST_MONTH = """
SELECT
    DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AS last_month
FROM
    kpi.revenue_kpi
ORDER BY 
    last_month DESC
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
WITH base_services AS (
    SELECT
        people.oid AS people_oid,
        services.pay_type,
        presc.id AS presc_id,
        ps.oid AS presc_service_oid
    FROM mir.people people
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
            SELECT 1
            FROM mir.sotr sotr
            JOIN mir.sysuser sysuser ON sotr.sysuser = sysuser.oid
            WHERE sotr.oid = presc.creator_id
              AND sysuser.oid = '5e95e526-907f-4eef-9093-ac0524a39f5b'
        )),
prices AS (
    SELECT
        bs.pay_type,
        bs.presc_service_oid,
        mir.get_price_by_presc(bs.presc_id, bs.presc_service_oid) AS base_price
    FROM base_services bs
),
discounts AS (
    SELECT
        bs.presc_service_oid,
        CASE
            WHEN NOT EXISTS (
                SELECT 1
                FROM pay.finance_plan_presc_service fpps
                JOIN pay.finance_plan plan ON plan.oid = fpps.finance_plan
                JOIN mir.visit vis ON vis.id = CAST(plan.visit AS bpchar(36))
                WHERE fpps.presc_service = bs.presc_service_oid
                  AND vis.pay_type_id = 'cash'
                  AND plan.fixed = true
                  AND CURRENT_DATE BETWEEN plan.date_begin AND plan.date_end
            )
            THEN SUM(COALESCE(dp.percent, d.percent))
        END AS sum_percent,
        CASE
            WHEN NOT EXISTS (
                SELECT 1
                FROM pay.finance_plan_presc_service fpps
                JOIN pay.finance_plan plan ON plan.oid = fpps.finance_plan
                JOIN mir.visit vis ON vis.id = CAST(plan.visit AS bpchar(36))
                WHERE fpps.presc_service = bs.presc_service_oid
                  AND vis.pay_type_id = 'cash'
                  AND plan.fixed = true
                  AND CURRENT_DATE BETWEEN plan.date_begin AND plan.date_end
            )
            THEN SUM(dp.fix_sum)
        END AS fix_sum_discount
    FROM base_services bs
    LEFT JOIN mir.discount_presc dp ON dp.presc_service = CAST(bs.presc_service_oid AS varchar)
    LEFT JOIN mir.discount d ON d.oid = dp.discount
    GROUP BY bs.presc_service_oid
),
round_setting AS (
    SELECT valuepar FROM mir.systemsettings WHERE param = 'DiscountAmountRounding'
)
SELECT
    SUM(
        CASE
            WHEN d.sum_percent IS NOT NULL THEN
                CASE rs.valuepar
                    WHEN '1' THEN CEILING(p.base_price - (p.base_price * d.sum_percent / 100) - COALESCE(d.fix_sum_discount, 0))
                    WHEN '2' THEN TRUNC(p.base_price - (p.base_price * d.sum_percent / 100) - COALESCE(d.fix_sum_discount, 0))
                    WHEN '3' THEN ROUND(p.base_price - (p.base_price * d.sum_percent / 100) - COALESCE(d.fix_sum_discount, 0))
                    ELSE ROUND(p.base_price - (p.base_price * d.sum_percent / 100) - COALESCE(d.fix_sum_discount, 0), 2)
                END
            ELSE p.base_price
        END
    ) AS revenue
FROM prices p
JOIN discounts d ON d.presc_service_oid = p.presc_service_oid
CROSS JOIN round_setting rs
"""


def add_month(date: datetime) -> datetime:
    if date.month == 12:
        result = date.replace(year=date.year + 1, month=1, day=1)
    else:
        result = date.replace(month=date.month + 1, day=1)
    return result


def month_ago(date: datetime) -> datetime:
    if date.month == 1:
        result = date.replace(year=date.year - 1, month=12, day=1)
    else:
        result = date.replace(month=date.month - 1, day=1)
    return result


def get_current_month_start() -> datetime:
    """Возвращает первый день текущего месяца (без времени)."""
    now = datetime.now()
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def get_max_date_of_revenue_kpi():
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    sql = GET_LAST_MONTH
    result = hook.get_first(sql)
    logging.info(f"Last month:, {result}")
    max_date = result[0] if result else None
    return max_date


def select_revenue(start_date: datetime, end_date: datetime) -> float:
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    sql = SELECT_REVENUE_KPI.format(start_month=start_str, end_month=end_str)
    result = hook.get_first(sql)
    revenue = result[0] if result and result[0] is not None else 0.0
    logging.info(f"Revenue for period {start_str} – {end_str}: {revenue}")
    return revenue


def update_revenue(month_date: datetime, revenue: float):
    hook = PostgresHook(postgres_conn_id="dwh_pg")
    month_str = month_date.strftime("%Y-%m-%d")
    sql = UPDATE_REVENUE_KPI.format(month=month_str, revenue=revenue)
    hook.run(sql)
    logging.info(f"Updated revenue for {month_str}: {revenue}")


def get_kpi_revenue(max_date):
    if max_date is None:
        start_date: datetime = (2025, 1, 1)
        logging.info("No previous data found. Starting from 2025-01-01")
    else:
        start_date = month_ago(max_date)
        if start_date < datetime(2025, 1, 1):
            start_date = datetime(2025, 1, 1)
        logging.info(f"Last month found: {max_date}. Starting from {start_date}")

    current_month = get_current_month_start()

    while start_date <= current_month():
        end_date = add_month(start_date)
        revenue = select_revenue(start_date, end_date)
        update_revenue(start_date, revenue)
        start_date = end_date

        logging.info("All months up to current have been processed.")


default_args = {
    "owner": "levchenko-an",
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="patientnet_etl",
    start_date=datetime(2026, 7, 27),
    schedule="0 0 * * *",
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
    get_last_month >> get_revenue
