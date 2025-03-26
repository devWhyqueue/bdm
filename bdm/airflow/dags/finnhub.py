from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

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

with DAG(
        'finnhub_crypto_news_scraper',
        default_args=default_args,
        description='Scrape crypto news from Finnhub API',
        schedule_interval='*/10 * * * *',  # Run every 10 minutes
        start_date=days_ago(1),
        catchup=False,
) as dag:
    scrape_finnhub_news = DockerOperator(
        task_id='scrape_finnhub_news',
        image='finnhub-news-scraper:latest',
        api_version='auto',
        auto_remove="force",
        environment=env_vars,
        docker_url='unix://var/run/docker.sock',
        network_mode='bdm_default'
    )
