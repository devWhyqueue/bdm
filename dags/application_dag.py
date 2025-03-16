
import textwrap
from airflow.models.dag import DAG
from airflow.models import Variable
# We use the docker operator for avoiding dependencies at system level -> they stay at task level
# Furthermore, it decouples the development of tasks and DAG
# Ref: https://marclamberti.com/blog/how-to-use-dockeroperator-apache-airflow/
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    "bi-system",
    default_args={
        "depends_on_past": False,
        "email": [""],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0, # we don't want automatic retries
        # "retry_delay": timedelta(minutes=5),
    },
    description="DAG of the real-time BI System for BDM",
    schedule=None, # we only trigger our system manually
    catchup=False,
    tags=["bi-system"],
) as dag:
    
    # streaming source task
    stock_stream_kafka_producer = DockerOperator(
        task_id='stock_stream_kafka_producer',
        image='stock_stream_kafka_producer_img',
        container_name='stock_stream_kafka_producer',
        api_version='auto',
        auto_remove='force',
        command="python stock_stream_kafka_producer.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bdm_default", # must be the name of the docker network
        environment={
          'API_KEY': Variable.get('FINAZON_API_KEY'), # insert api key in .env
          'FREQUENCY': '1s',
          'TICKER': 'AAPL',
          'KAFKA_ENDPOINT': 'kafka:9092'
        }
    )
    stock_stream_kafka_producer.doc_md = textwrap.dedent(
        """\
    #### Task Documentation
    This task executes the stock stream kafka producer python script in a separate environment.
    """
    )

    dag.doc_md = """
    ### This is the dag of our real-time BI system
    """

    # space for dependency specifications
    # WIP
