# Как использовать XCom в Airflow

https://www.notion.so/korsak0v/Data-Engineer-185c62fdf79345eb9da9928356884ea0

## О видео

## О проекте

### Виртуальное окружение

Настройка виртуального окружения:

```bash
python3.12 -m venv venv && \
source venv/bin/activate && \
pip install --upgrade pip && \
pip install poetry && \
poetry lock && \
poetry install
```

### Настройка Airflow через Docker

Мы используем Airflow, который собирается при помощи [Dockerfile](Dockerfile)
и [docker-compose.yaml](docker-compose.yaml).

Для запуска контейнера с Airflow, выполните команду:

```bash
docker-compose up -d
```

Веб-сервер Airflow запустится на хосте http://localhost:8080/, если не будет работать данный хост, то необходимо перейти
по хосту http://0.0.0.0:8080/.

#### Добавление пакетов в текущую сборку

Для того чтобы добавить какой-то пакет в текущую сборку, необходимо выполнить следующие шаги:

* Добавить новую строку в [Dockerfile](Dockerfile)
* Выполнить команду:

```bash
docker-compose build
```

* Выполнить команду:

```bash
docker-compose up -d
```

### DeBug класса TaskInstance

Создай новый DAG или измени текущий – [simple_push_xcom_values.py](dags/simple_push_xcom_values.py):

Поставь точку остановы в конце кода и запусти отладку.

Затем ты сможешь увидеть созданный объект TaskInstance и его атрибуты.

```python
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "i.korsakov"
DAG_ID = "simple_push_xcom_values"

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


def simple_push_xcom(**context) -> pendulum.DateTime:
    """
    Возвращает дату из контекста DAG.

    @param context: Контекст DAG.
    @return: data_interval_start:pendulum.DateTime.
    """

    return context.get("data_interval_start")


with DAG(
        dag_id=DAG_ID,
        schedule="0 10 * * *",
        default_args=args,
        tags=["xcom"],
        description=SHORT_DESCRIPTION,
        max_active_tasks=1,
        max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    simple_push_xcom = PythonOperator(
        task_id="simple_push_xcom",
        python_callable=simple_push_xcom,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> simple_push_xcom >> end

if __name__ == "__main__":
    import uuid
    from airflow.utils.state import State
    from airflow.utils.types import DagRunType
    from airflow.models import DagRun
    from airflow.utils.session import create_session

    task = dag.get_task("simple_push_xcom")

    with create_session() as session:
        dag_run = DagRun(
            dag_id=dag.dag_id,
            run_id=f"manual_debug_{str(uuid.uuid4())}",
            execution_date=pendulum.now("UTC"),
            start_date=pendulum.now("UTC"),
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
        )
        session.add(dag_run)
        session.commit()

        # Создаем TaskInstance
        from airflow.models import TaskInstance

        ti = TaskInstance(task=task, run_id=dag_run.run_id)
        ti.state = State.RUNNING

        print(f"TaskInstance created: {ti}")
        print(f"ti.task_id: {ti.task_id}")
        print(f"ti.dag_id: {ti.dag_id}")
        print(f"ti.state: {ti.state}")
```