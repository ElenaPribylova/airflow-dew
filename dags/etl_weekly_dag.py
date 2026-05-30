"""
Практическое задание: еженедельный ETL-DAG.
Секреты НЕ хранятся в коде, они в Airflow Connections.
"""

from datetime import timedelta
import csv
import io
import ast

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

UNIQUE_ID = "lena_p77" # мой уникальный идентификатор
PG_CONN_ID = "postgresID" 
MINIO_CONN_ID = "MinioID" 

# Параметры API
API_URL = "https://b2b.itresume.ru/api/statistics"
CLIENT = "Skillfactory"
CLIENT_KEY = "M2MGWS"

# Бакет в Minio, куда кладём CSV
MINIO_BUCKET = "reports"
TEST_START = "2024-11-01"
TEST_END = "2024-12-01"

RAW_TABLE = f"{UNIQUE_ID}_raw"
AGG_TABLE = f"{UNIQUE_ID}_agg"


def _pg_connect():
    """Открывает соединение с Postgres по данным из Airflow Connection."""
    import psycopg2
    c = BaseHook.get_connection(PG_CONN_ID)
    return psycopg2.connect(
        host=c.host,
        port=c.port or 5432,
        dbname=c.schema,
        user=c.login,
        password=c.password,
    )


def _period(context):
    """Возвращает (start, end) как строки 'YYYY-MM-DD'."""
    if TEST_START and TEST_END:
        return TEST_START, TEST_END
    start = context["data_interval_start"].strftime("%Y-%m-%d")
    end = context["data_interval_end"].strftime("%Y-%m-%d")
    return start, end


def create_tables(**context):
    """Шаг 0: создаём обе таблицы, если их ещё нет."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
        lti_user_id TEXT,
        is_correct BOOLEAN,
        attempt_type TEXT,
        created_at TIMESTAMP,
        oauth_consumer_key TEXT,
        lis_result_sourcedid TEXT,
        lis_outcome_service_url TEXT
    );
    CREATE TABLE IF NOT EXISTS {AGG_TABLE} (
        period_start DATE,
        period_end DATE,
        total_attempts INTEGER,
        unique_users INTEGER,
        correct_attempts INTEGER,
        success_rate NUMERIC,
        min_created_at TIMESTAMP,
        max_created_at TIMESTAMP,
        computed_at TIMESTAMP
    );
    """
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def extract_and_load(**context):
    """Шаги 1-2: тянем API, разбираем JSON, идемпотентно пишем в RAW."""
    import requests
    start, end = _period(context)

    params = {"client": CLIENT, "client_key": CLIENT_KEY, "start": start, "end": end}
    resp = requests.get(API_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for item in data:
        # passback_params — это строка с python-словарём, разбираем безопасно
        pb = item.get("passback_params") or "{}"
        try:
            pb_dict = ast.literal_eval(pb)
            if not isinstance(pb_dict, dict):
                pb_dict = {}
        except (ValueError, SyntaxError):
            pb_dict = {}

        rows.append((
            item.get("lti_user_id"),
            item.get("is_correct"),
            item.get("attempt_type"),
            item.get("created_at"),
            pb_dict.get("oauth_consumer_key"),
            pb_dict.get("lis_result_sourcedid"),
            pb_dict.get("lis_outcome_service_url"),
        ))

    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            # удаляем данные за этот же период перед вставкой,
            # чтобы повторный запуск не плодил дубли.
            cur.execute(
                f"DELETE FROM {RAW_TABLE} WHERE created_at >= %s AND created_at < %s",
                (start, end),
            )
            if rows:
                cur.executemany(
                    f"""INSERT INTO {RAW_TABLE}
                        (lti_user_id, is_correct, attempt_type, created_at,
                         oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    rows,
                )
        conn.commit()
    finally:
        conn.close()

    print(f"Загружено строк: {len(rows)} за период {start}..{end}")


def aggregate(**context):
    """Шаг 3: считаем средние/мин/макс/счётчики и пишем в AGG (идемпотентно)."""
    start, end = _period(context)
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT lti_user_id),
                    COUNT(*) FILTER (WHERE is_correct IS TRUE),
                    AVG(CASE WHEN is_correct THEN 1 ELSE 0 END),
                    MIN(created_at),
                    MAX(created_at)
                FROM {RAW_TABLE}
                WHERE created_at >= %s AND created_at < %s
                """,
                (start, end),
            )
            total, users, correct, rate, min_ts, max_ts = cur.fetchone()

            cur.execute(f"DELETE FROM {AGG_TABLE} WHERE period_start = %s", (start,))
            cur.execute(
                f"""INSERT INTO {AGG_TABLE}
                    (period_start, period_end, total_attempts, unique_users,
                     correct_attempts, success_rate, min_created_at, max_created_at, computed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                (start, end, total or 0, users or 0, correct or 0, rate, min_ts, max_ts),
            )
        conn.commit()
    finally:
        conn.close()

    print(f"Агрегат: attempts={total}, users={users}, correct={correct}")


def export_csv(**context):
    """Шаг 4: выгружаем агрегаты в CSV и кладём в Minio. Требует boto3."""
    import boto3
    start, end = _period(context)

    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {AGG_TABLE} WHERE period_start = %s", (start,))
            cols = [d[0] for d in cur.description]
            data = cur.fetchall()
    finally:
        conn.close()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(cols)
    writer.writerows(data)
    csv_bytes = buf.getvalue().encode("utf-8")

    m = BaseHook.get_connection(MINIO_CONN_ID)
    s3 = boto3.client(
        "s3",
        endpoint_url=m.host, # напр. http://95.163.241.236:9000
        aws_access_key_id=m.login,
        aws_secret_access_key=m.password,
    )
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if MINIO_BUCKET not in existing:
        s3.create_bucket(Bucket=MINIO_BUCKET)

    key = f"{UNIQUE_ID}/agg_{start}_{end}.csv"
    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=csv_bytes)
    print(f"CSV выгружен в Minio: {MINIO_BUCKET}/{key}")


default_args = {
    "owner": UNIQUE_ID,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=f"etl_weekly_{UNIQUE_ID}",
    description="Еженедельный ETL: API -> Postgres -> агрегаты -> CSV в Minio",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@weekly", # запуск раз в неделю
    catchup=False, # не досчитываем все прошлые недели
    tags=["practice", "etl", UNIQUE_ID],
) as dag:

    start = EmptyOperator(task_id="start")
    t_create = PythonOperator(task_id="create_tables", python_callable=create_tables)
    t_extract = PythonOperator(task_id="extract_and_load", python_callable=extract_and_load)
    t_aggregate = PythonOperator(task_id="aggregate", python_callable=aggregate)
    t_export = PythonOperator(task_id="export_csv", python_callable=export_csv)
    end = EmptyOperator(task_id="end")

    start >> t_create >> t_extract >> t_aggregate >> t_export >> end