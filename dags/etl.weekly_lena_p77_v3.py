"""
Кастомный PostgresOperator + DAG с агрегацией через него.
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
from airflow.models import BaseOperator

UNIQUE_ID = "lena_p77"
PG_CONN_ID = "postgresID"
MINIO_CONN_ID = "MinioID"

API_URL = "https://b2b.itresume.ru/api/statistics"
CLIENT = "Skillfactory"
CLIENT_KEY = "M2MGWS"

MINIO_BUCKET = "reports"

RAW_TABLE = f"{UNIQUE_ID}_raw"
AGG_TABLE = f"{UNIQUE_ID}_agg"

""" 
Кастомный оператор
"""

class PostgresOperator(BaseOperator):
    """
    Кастомный оператор для выполнения INSERT/UPDATE/DELETE/DDL
    запросов к PostgreSQL. SELECT не поддерживается — оператор
    ничего не возвращает.

    :param sql: SQL-запрос или список запросов (Jinja-шаблоны поддерживаются)
    :param conn_id: ID соединения Airflow с Postgres
    :param parameters: параметры для подстановки в запрос (опционально)
    :param autocommit: выполнять коммит автоматически (по умолчанию True)
    """

    # Указываем Airflow, что поле sql поддерживает Jinja-шаблоны
    template_fields = ("sql", "parameters")

    def __init__(
        self,
        sql,
        conn_id: str = PG_CONN_ID,
        parameters=None,
        autocommit: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.parameters = parameters or []
        self.autocommit = autocommit

    def execute(self, context):
        import psycopg2

        c = BaseHook.get_connection(self.conn_id)
        conn = psycopg2.connect(
            host=c.host,
            port=c.port or 5432,
            dbname=c.schema,
            user=c.login,
            password=c.password,
        )
        try:
            with conn.cursor() as cur:
                # Поддержка как одного запроса, так и списка
                queries = self.sql if isinstance(self.sql, list) else [self.sql]
                for query in queries:
                    self.log.info("Выполняю запрос: %s", query)
                    cur.execute(query, self.parameters or None)
            if self.autocommit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

        self.log.info("Запрос выполнен успешно.")

"""
Вспомогательные функции
"""

def _pg_connect():
    import psycopg2
    c = BaseHook.get_connection(PG_CONN_ID)
    return psycopg2.connect(
        host=c.host,
        port=c.port or 5432,
        dbname=c.schema,
        user=c.login,
        password=c.password,
    )


def create_tables(start, end, **context):
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


def extract_and_load(start, end, **context):
    import requests

    params = {"client": CLIENT, "client_key": CLIENT_KEY, "start": start, "end": end}
    resp = requests.get(API_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for item in data:
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


def export_csv(start, end, **context):
    import boto3

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
        endpoint_url=m.host,
        aws_access_key_id=m.login,
        aws_secret_access_key=m.password,
    )
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if MINIO_BUCKET not in existing:
        s3.create_bucket(Bucket=MINIO_BUCKET)

    key = f"{UNIQUE_ID}/agg_{start}_{end}.csv"
    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=csv_bytes)
    print(f"CSV выгружен в Minio: {MINIO_BUCKET}/{key}")


"""
DAG
"""

default_args = {
    "owner": UNIQUE_ID,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=f"etl_weekly_{UNIQUE_ID}_v3",
    description="Урок 13: агрегация через кастомный PostgresOperator",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@weekly",
    catchup=False,
    tags=["practice", "etl", "custom_operator", UNIQUE_ID],
) as dag:

    start_op = EmptyOperator(task_id="start")

    t_create = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
        op_kwargs={
            "start": "{{ data_interval_start | ds }}",
            "end": "{{ data_interval_end | ds }}",
        },
    )

    t_extract = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
        op_kwargs={
            "start": "{{ data_interval_start | ds }}",
            "end": "{{ data_interval_end | ds }}",
        },
    )

    #  Агрегация теперь через кастомный PostgresOperator
    t_aggregate = PostgresOperator(
        task_id="aggregate",
        conn_id=PG_CONN_ID,
        sql=[
            # Сначала удаляем старые данные за период 
            f"""
            DELETE FROM {AGG_TABLE}
            WHERE period_start = '{{{{ data_interval_start | ds }}}}'
            """,
            # Затем вставляем свежие агрегаты
            f"""
            INSERT INTO {AGG_TABLE}
                (period_start, period_end, total_attempts, unique_users,
                 correct_attempts, success_rate, min_created_at, max_created_at, computed_at)
            SELECT
                '{{{{ data_interval_start | ds }}}}'::DATE,
                '{{{{ data_interval_end | ds }}}}'::DATE,
                COUNT(*),
                COUNT(DISTINCT lti_user_id),
                COUNT(*) FILTER (WHERE is_correct IS TRUE),
                AVG(CASE WHEN is_correct THEN 1.0 ELSE 0.0 END),
                MIN(created_at),
                MAX(created_at),
                NOW()
            FROM {RAW_TABLE}
            WHERE created_at >= '{{{{ data_interval_start | ds }}}}'
              AND created_at < '{{{{ data_interval_end | ds }}}}'
            """,
        ],
    )

    t_export = PythonOperator(
        task_id="export_csv",
        python_callable=export_csv,
        op_kwargs={
            "start": "{{ data_interval_start | ds }}",
            "end": "{{ data_interval_end | ds }}",
        },
    )

    end_op = EmptyOperator(task_id="end")

    start_op >> t_create >> t_extract >> t_aggregate >> t_export >> end_op