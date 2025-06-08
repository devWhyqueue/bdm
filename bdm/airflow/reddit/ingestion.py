from datetime import datetime

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


def create_scraper_task(subreddit, sort_by='new', time_filter='all',
                        limit=25, prefix='', task_id=None):
    """Create a DockerOperator task for a specific subreddit configuration."""
    if task_id is None:
        task_id = f"scrape_{subreddit}_{sort_by}"

    env_vars = {
        'REDDIT_CLIENT_ID': '{{ conn.reddit_api.login }}',
        'REDDIT_CLIENT_SECRET': '{{ conn.reddit_api.password }}',
        'REDDIT_USER_AGENT': 'subreddit_scraper v1.0',

        'MINIO_ENDPOINT': '{{ conn.s3.extra_dejson.host }}',
        'MINIO_ACCESS_KEY': '{{ conn.s3.login }}',
        'MINIO_SECRET_KEY': '{{ conn.s3.password }}',
        'MINIO_BUCKET': 'landing-zone',
    }

    cmd_args = [
        f"--subreddit={subreddit}",
        f"--limit={limit}",
        f"--sort-by={sort_by}",
        f"--time-filter={time_filter}"
    ]

    if prefix:
        cmd_args.append(f"--prefix={prefix}")

    return DockerOperator(
        task_id=task_id,
        image='subreddit-scraper:latest',
        container_name=f'{task_id}_{{{{ds}}}}',
        api_version='auto',
        auto_remove="force",
        docker_url='unix://var/run/docker.sock',
        network_mode="bdm_default",
        environment=env_vars,
        command=cmd_args,
        do_xcom_push=True,  # Push stdout (filename) to XComs
    )


# Define the DAG using TaskFlow API
@dag(
    dag_id='reddit_ingestion',
    default_args=default_args,
    description='Scrape various subreddits and trigger processing DAG (TaskFlow API)',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 3, 22),
    catchup=False,
    tags=['reddit', 'ingestion', 'scraper', 'taskflow'],
)
def subreddit_scraper_dag():
    """Defines the subreddit scraping DAG using TaskFlow API."""

    # Bitcoin - new posts every minute
    bitcoin_task = create_scraper_task(
        subreddit='bitcoin',
        sort_by='new',
        prefix='{{ ds }}',
    )

    trigger_reddit_processing_dag = TriggerDagRunOperator(
        task_id="trigger_reddit_processing",
        trigger_dag_id="reddit_processing",  # DAG ID of the processing DAG
        conf={
            "logical_date": "{{ ds }}",
            "input_filename": "{{ ti.xcom_pull(task_ids='scrape_bitcoin_new') }}"
        },  # Pass execution date and filename to the triggered DAG
        wait_for_completion=False,  # Set to True if this DAG should wait for the triggered DAG
        poke_interval=30,  # How often to check status if wait_for_completion=True
    )

    # Set task dependency
    bitcoin_task >> trigger_reddit_processing_dag


# Instantiate the DAG to make it discoverable by Airflow
subreddit_scraper_dag_instance = subreddit_scraper_dag()
