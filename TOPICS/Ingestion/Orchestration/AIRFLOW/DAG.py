from airflow.decorators import dag
from airflow import DAG
import pendulum

# dag1 - using @dag decorator
@dag(
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"]
)
def educative_dag1():
    pass

educative_dag1()

# dag2 - using context manager
with DAG(
    dag_id="educative_dag2",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"]    
) as dag2:
    pass



# dag3
@dag(
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"]
)
def educative_dag3():

    s1 = TimeDeltaSensor(task_id="time_sensor", delta=timedelta(seconds=2))
    o1 = BashOperator(task_id="bash_operator", bash_command="echo run a bash script")
    @task
    def python_operator() -> None:
        logging.info("run a python function")
    o2 = python_operator()

    s1 >> o1 >> o2

educative_dag3()


@dag(
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"]
)
def educative_dag4():

    @task
    def python_operator() -> None:
        logging.info("run a python function")
        return str(datetime.now()) # return value automatically stores in XCOMs
    o1 = python_operator()
    o2 = BashOperator(task_id="bash_operator1", bash_command='echo "{{ ti.xcom_pull(task_ids="python_operator") }}"') # traditional way to retrieve XCOM value
    o3 = BashOperator(task_id="bash_operator2", bash_command=f'echo {o1}') # make use of @task feature
    
    o1 >> o2 >> o3

educative_dag4()


def days_to_now(starting_date):
    return (datetime.now() - starting_date).days

@dag(
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"],
    user_defined_macros={
        "starting_date": datetime(2015, 5, 1),
        "days_to_now": days_to_now, 
    })
def educative_dag5():

    o1 = BashOperator(task_id="bash_operator1", bash_command="echo Today is {{ execution_date.format('dddd') }}")
    o2 = BashOperator(task_id="bash_operator2", bash_command="echo Days since {{ starting_date }} is {{ days_to_now(starting_date) }}")

    o1 >> o2

educative_dag5()



@dag(
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"],
    template_searchpath=["/usercode/dags/sql"])
def educative_dag6():

    BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": "{% include 'sample.sql' %}",
                "useLegacySql": False,
            }
        }
    )

educative_dag6()



from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"]
)
def educative_dag7():

    TriggerDagRunOperator(
        task_id="trigger_dagrun",
        trigger_dag_id="educative_dag1", 
        conf={},
    )

educative_dag7()




from airflow.sensors.external_task import ExternalTaskSensor

@dag(
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"]
)
def educative_dag8():

    ExternalTaskSensor(
        task_id="external_sensor",
        external_dag_id="educative_dag3",
        external_task_id="python_operator",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

educative_dag8()



dag1_dataset = Dataset("s3://dag1/output_1.txt", extra={"hi": "bye"})

@dag(
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"]
)
def educative_dag9_producer():
    BashOperator(outlets=[dag1_dataset], task_id="producer", bash_command="sleep 5")

educative_dag9_producer()

@dag(
    schedule=[dag1_dataset],
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"]
)
def educative_dag9_consumer():
    BashOperator(task_id="consumer", bash_command="sleep 5")

educative_dag9_consumer()



# Bad example - requests will be made every 30 seconds instead of everyday at 4:30am
res = requests.get("https://api.sampleapis.com/coffee/hot")

@dag(
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"]
)
def educative_dag7():

    @task
    def python_operator() -> None:
        logging.info(f"API result {res}")
    python_operator()

educative_dag7()

# Good example

@dag(
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"]
)
def educative_dag7():

    @task
    def python_operator() -> None:
        res = requests.get("https://api.sampleapis.com/coffee/hot") # move API request within DAG context
        logging.info(f"API result {res}")
    python_operator()

educative_dag7()