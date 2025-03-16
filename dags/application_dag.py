
import textwrap
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models import Variable

# # Operators; we need this to operate!
# from airflow.operators.bash import BashOperator
# The extra docker operator
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    "bi-system",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": [""],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0, # we don't want automatic retries
        # "retry_delay": timedelta(minutes=5),
    },
    description="DAG of the real-time BI System for BDM",
    schedule=None, # only 
    # start_date=datetime(2021, 1, 1), # skip because we want to trigger it manually
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
          'API_KEY': Variable.get('FINAZON_API_KEY'), # insert api key
          'FREQUENCY': '1s',
          'TICKER': 'AAPL',
          'KAFKA_ENDPOINT': 'kafka:9092'
        }
    )
    stock_stream_kafka_producer.doc_md = textwrap.dedent(
        """\
    #### Task Documentation
    This task executes the stock stream kafka producer python script in a separate container.
    """
    )

    dag.doc_md = """
    ### This is the dag of our real-time BI system
    """

    # space for dependency specifications
