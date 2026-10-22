import logging
import os
import random
import re
import time
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.trigger_rule import TriggerRule


CONNECTION_ID = "hw07_mysql"
SCHEMA = os.environ.get("HW07_SCHEMA", "aurika_hryhorova")
if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", SCHEMA):
    raise ValueError("HW07_SCHEMA must be a plain MySQL schema name")
if SCHEMA.lower() in {"olympic_dataset", "mysql", "sys", "information_schema", "performance_schema"}:
    raise ValueError("HW07_SCHEMA must be a personal learning schema")

TABLE = f"`{SCHEMA}`.`aurika_hw07_medal_counts`"
MEDALS = ("Bronze", "Silver", "Gold")


def mysql_hook():
    return MySqlHook(mysql_conn_id=CONNECTION_ID)


def create_table():
    mysql_hook().run(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(6) NOT NULL,
            `count` BIGINT UNSIGNED NOT NULL,
            created_at DATETIME(6) NOT NULL
        )
    """)


def choose_medal():
    medal = random.choice(MEDALS)
    logging.info("Selected medal: %s", medal)
    return medal


def choose_branch(ti):
    medal = ti.xcom_pull(task_ids="choose_medal")
    if medal not in MEDALS:
        raise ValueError("Unknown medal")
    return f"count_{medal.lower()}"


def count_medals(medal):
    if medal not in MEDALS:
        raise ValueError("Unknown medal")
    connection = mysql_hook().get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = %s",
                (medal,),
            )
            count = int(cursor.fetchone()[0])
            cursor.execute(
                f"INSERT INTO {TABLE} (medal_type, `count`, created_at) VALUES (%s, %s, UTC_TIMESTAMP(6))",
                (medal, count),
            )
            row_id = int(cursor.lastrowid)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    logging.info("Inserted id=%s, medal=%s, count=%s; created_at uses UTC", row_id, medal, count)
    return row_id


def delay(params):
    seconds = params["delay_seconds"]
    if isinstance(seconds, bool) or not isinstance(seconds, int) or not 0 <= seconds <= 60:
        raise ValueError("delay_seconds must be an integer from 0 to 60")
    logging.info("Sleeping for %s seconds", seconds)
    time.sleep(seconds)


def is_fresh(age_microseconds):
    return age_microseconds is not None and 0 <= age_microseconds <= 30_000_000


def is_invalid_age(age_microseconds):
    age_seconds = age_microseconds / 1_000_000 if age_microseconds is not None else None
    logging.info("Age of this run's row: %s seconds; allowed: 0..30", age_seconds)
    return age_microseconds is None or age_microseconds < 0 or age_microseconds > 30_000_000


with DAG(
    dag_id="aurika_hw07_medals",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=3),
    default_args={"owner": "aurika", "retries": 0, "execution_timeout": timedelta(seconds=90)},
    params={"delay_seconds": Param(5, type="integer", minimum=0, maximum=60)},
    tags=["homework", "hw07"],
) as dag:
    create = PythonOperator(task_id="create_table", python_callable=create_table)
    choose = PythonOperator(task_id="choose_medal", python_callable=choose_medal)
    branch = BranchPythonOperator(task_id="branch", python_callable=choose_branch)
    branches = [
        PythonOperator(
            task_id=f"count_{medal.lower()}",
            python_callable=count_medals,
            op_kwargs={"medal": medal},
        )
        for medal in MEDALS
    ]
    wait = PythonOperator(
        task_id="delay",
        python_callable=delay,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    sensor = SqlSensor(
        task_id="check_freshness",
        conn_id=CONNECTION_ID,
        sql=f"""
            SELECT TIMESTAMPDIFF(MICROSECOND, created_at, UTC_TIMESTAMP(6))
            FROM {TABLE}
            WHERE id = %s AND medal_type = %s
            ORDER BY id DESC
            LIMIT 1
        """,
        parameters=[
            "{{ ti.xcom_pull(task_ids='count_' + ti.xcom_pull(task_ids='choose_medal').lower()) }}",
            "{{ ti.xcom_pull(task_ids='choose_medal') }}",
        ],
        success=is_fresh,
        failure=is_invalid_age,
        fail_on_empty=True,
        mode="poke",
        poke_interval=1,
        timeout=10,
        execution_timeout=timedelta(seconds=20),
        soft_fail=False,
    )

    create >> choose >> branch >> branches >> wait >> sensor
