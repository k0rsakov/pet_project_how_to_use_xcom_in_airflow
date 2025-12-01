import logging

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Конфигурация DAG
OWNER = "i.korsakov"
DAG_ID = "simple_dag"

LONG_DESCRIPTION = """
# LONG DESCRIPTION

"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

# Описание возможных ключей для default_args
# https://github.com/apache/airflow/blob/343d38af380afad2b202838317a47a7b1687f14f/airflow/example_dags/tutorial.py#L39
args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(year=2025, month=1, day=1, tz="UTC"),
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def simple_task(**context) -> None:
    """
    Печатает контекст DAG.

    @param context: Контекст DAG.
    @return: Ничего не возвращает.
    """

    for context_key, context_key_value in context.items():
        logging.info(
            f"key_name – {context_key} | "
            f"value_name – {context_key_value} | "
            f"type_value_name – {type(context_key_value)}",
        )


with DAG(
    dag_id=DAG_ID,
    schedule="0 10 * * *",
    default_args=args,
    tags=["context"],
    description=SHORT_DESCRIPTION,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    simple_task = PythonOperator(
        task_id="simple_task",
        python_callable=simple_task,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> simple_task >> end
