# Как использовать XCom в Airflow

https://www.notion.so/korsak0v/Data-Engineer-185c62fdf79345eb9da9928356884ea0

## О видео

🔥 Хочешь понять, как передавать данные между задачами в Apache Airflow, не запутаться в XCom’ах и не превратить
metastore в свалку?
В этом [видео](https://youtu.be/DGUwzrJn_ag) разберём XCom в Airflow с нуля на живых примерах DAG’ов: как пушить и
пуллить значения, когда XCom — это удобно, а когда это прямой путь к боли и антипаттернам.

Ссылки:
Менторство/консультации по Data
Engineering — https://korsak0v.notion.site/Data-Engineer-185c62fdf79345eb9da9928356884ea0
TG-канал — https://t.me/DataLikeQWERTY
Instagram — https://www.instagram.com/i__korsakov/
Habr — https://habr.com/ru/users/k0rsakov/publications/articles/

🔍 Что в видео:

- 🧠 Что такое XCom в Apache Airflow и зачем вообще передавать данные между задачами DAG
- ⚙️ Простой DAG simple_push_xcom_values: как создаётся XCom «по умолчанию» и где его посмотреть в Airflow UI
- 🔎 Где живут XCom’ы:
    - вкладка XCom в веб-интерфейсе Airflow
    - таблица xcom в метасторе через DBeaver
- 🧪 DAG simple_dag:
    - что такое task_instance (ti) в контексте задачи
    - почему лучше явно использовать task_instance, а не полагаться на «магический» ti
    - как TaskInstance связан с текущим DAGRun и состоянием задач
    - почему стандартный Debug в IDE не даёт полноценного TaskInstance и как заглянуть в «кишки» XCom’ов через код
- 🔄 DAG simple_communications_between_tasks:
    - как одна задача пушит значение через xcom_push, а другая забирает его через xcom_pull
    - чем отличается ключ XCom от имени таски и зачем задавать свой key
    - как по логам увидеть, что одна задача ничего не возвращает, а другая всё равно получает значение из XCom
- 🎯 DAG simple_communications_between_tasks_with_target_values:
    - функция-таска get_common_date как единый источник общей даты для всего пайплайна
    - как написать таску, которая пушит данные в XCom и потом забирать их по имени функции / таски
    - пример context.get("task_instance").xcom_pull("get_common_date") и разбор, что здесь реально происходит под
      капотом
- 🚫 Почему не стоит злоупотреблять XCom:
    - сложно дебажить и тестировать такой код, особенно в проде
    - сильная зависимость от Airflow и метастора: без них логика не воспроизводится
    - метастор засоряется XCom-записями, значения живут «вечно», если их не чистить
    - как быстро растут объёмы записей при десятках / сотнях DAG’ов и частых запусках
    - где посмотреть ограничения по размеру XCom (файл xcom.py в исходниках Airflow)
    - когда стоит переходить на Custom XCom Backend и внешние хранилища
- ✅ Когда XCom действительно полезен:
    - передача статусов и сигналов между задачами (доступность API, состояние БД, флаги проверок)
    - отметка этапов пайплайна как «выполнено», чтобы повысить прозрачность выполнения DAG
    - хранение небольших служебных значений, а не данных уровня DWH
- 💣 Антипаттерны при работе с XCom:
    - передавать параметры и данные между разными DAG’ами через XCom
    - складывать большие объёмы данных в XCom вместо нормального хранилища
    - использовать XCom как универсальную БД «на все случаи жизни»
    - игнорировать чистку XCom и рост метастора, пока он не начнёт замедлять Airflow

🗂️ GitHub репозиторий с кодом: https://github.com/k0rsakov/pet_project_how_to_use_xcom_in_airflow

✉️ Вопросы, обучение, консультации по Data Engineering и Apache Airflow — пиши в
личку: https://korsak0v.notion.site/Data-Engineer-185c62fdf79345eb9da9928356884ea0

💡 В конце видео — практические рекомендации: когда XCom — хороший инструмент для обмена статусами и небольшими
значениями между задачами,
а когда лучше спроектировать пайплайн так, чтобы данные жили во внешнем хранилище, а Airflow отвечал только за
оркестрацию.

Таймкоды:

- 00:00 – Начало
- 00:15 – Про инфраструктуру
- 01:24 – Простой DAG по отправке значений в XCom
- 04:11 – Что такое TaskInstance / ti
- 06:49 – Простой пример взаимодействия с XCom внутри DAG
- 11:04 – Боевой пример применения XCom внутри DAG для работы с API
- 16:48 – Рекомендации

#apacheairflow #airflow #xcom #airflowxcom #dag #apacheairflowtutorial #airflowtutorial #airflowforbeginners
#dataengineering #etl #elt #dwh #python #dataengineer #junior #middle #pipeline

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