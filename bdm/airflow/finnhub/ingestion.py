from datetime import datetime

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Environment variables to pass to the container
env_vars = {
    'FINNHUB_API_KEY': '{{ var.value.finnhub_api_key }}',
    'NEWS_CATEGORY': 'crypto',
    'MIN_ID': '0',
    'MINIO_ENDPOINT': '{{ conn.s3.extra_dejson.host }}',
    'MINIO_ACCESS_KEY': '{{ conn.s3.login }}',
    'MINIO_SECRET_KEY': '{{ conn.s3.password }}',
    'MINIO_BUCKET': 'landing-zone',
}


@dag(
    dag_id='finnhub_crypto_news_scraper',
    default_args=default_args,
    description='Scrape crypto news from Finnhub API and trigger processing DAG',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['finnhub', 'ingestion', 'scraper', 'taskflow'],
)
def finnhub_crypto_news_scraper_dag():
    """Defines the Finnhub crypto news scraping and processing trigger DAG."""
    scrape_finnhub_news = DockerOperator(
        task_id='scrape_finnhub_news',
        image='finnhub-news-scraper:latest',
        container_name='scrape_finnhub_news_{{ ds_nodash }}',
        api_version='auto',
        auto_remove="force",
        environment=env_vars,
        docker_url='unix://var/run/docker.sock',
        network_mode='bdm_default',
        do_xcom_push=True,
    )

    trigger_finnhub_processing = TriggerDagRunOperator(
        task_id="trigger_finnhub_processing",
        trigger_dag_id="finnhub_processing",
        conf={
            "logical_date": "{{ ds }}",
            "input_filename": "{{ ti.xcom_pull(task_ids='scrape_finnhub_news') }}",
            "landing_zone_bucket": env_vars['MINIO_BUCKET']
        },
        wait_for_completion=False,
        poke_interval=30,
    )

    scrape_finnhub_news >> trigger_finnhub_processing


# Instantiate the DAG
finnhub_crypto_news_scraper_dag_instance = finnhub_crypto_news_scraper_dag()
