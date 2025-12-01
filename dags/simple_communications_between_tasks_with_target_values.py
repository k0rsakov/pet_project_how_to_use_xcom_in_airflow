import logging
import requests
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Конфигурация DAG
OWNER = "i.korsakov"
DAG_ID = "simple_communications_between_tasks_with_target_values"

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


def get_common_date(**context) -> pendulum.DateTime:
    """
    Возвращает дату из контекста DAG.

    @param context: Контекст DAG.
    @return: data_interval_start:pendulum.DateTime.
    """
    return context.get("data_interval_start").format("YYYYMMDDHHmmss")


def simple_push_xcom(**context) -> None:
    """
    Возвращает дату из контекста DAG.

    @param context: Контекст DAG.
    @return: data_interval_start:pendulum.DateTime.
    """

    date_context = context.get("task_instance").xcom_pull("get_common_date")

    response = requests.get(url="https://catfact.ninja/fact", timeout=10)

    context.get("task_instance").xcom_push(key=f"cat_fact_{date_context}", value=response.json())
    logging.info(f"⬆️ Pushed cat fact success.")


def simple_pull_xcom(**context) -> None:
    """
    Извлекает дату из XCom и логирует её.

    :param context: Контекст DAG.
    :return: Ничего не возвращает.
    """

    date_context = context.get("task_instance").xcom_pull("get_common_date")

    date = context.get("task_instance").xcom_pull(task_ids="simple_push_xcom", key=f"cat_fact_{date_context}")

    logging.info(f"️⬇️ Pulled cat fact: {date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 10 * * *",
    default_args=args,
    tags=["xcom"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_common_date = PythonOperator(
        task_id="get_common_date",
        python_callable=get_common_date,
    )

    simple_push_xcom = PythonOperator(
        task_id="simple_push_xcom",
        python_callable=simple_push_xcom,
    )

    simple_pull_xcom = PythonOperator(
        task_id="simple_pull_xcom",
        python_callable=simple_pull_xcom,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_common_date >> simple_push_xcom >> simple_pull_xcom >> end
