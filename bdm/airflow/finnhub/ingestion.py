import re
from datetime import datetime

from airflow.decorators import dag, task
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
    dag_id='finnhub_news_ingestion',
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
        xcom_all=True,
    )

    @task
    def extract_output_filename(task_instance=None):
        """Extract the output filename from the logs of the Docker container."""
        logs = task_instance.xcom_pull(task_ids='scrape_finnhub_news')

        if not isinstance(logs, list):
            return None

        # Look for the successfully saved data log line
        save_pattern = r"Successfully saved data to (.+)"

        for entry in logs:
            if not entry:
                continue

            text = str(entry)
            match = re.search(save_pattern, text)
            if match:
                filepath = match.group(1).strip()
                # Get just the filename without the full s3a:// path
                if filepath.startswith('s3a://'):
                    # Extract the part after the bucket name
                    parts = filepath.split('/', 3)
                    if len(parts) >= 4:
                        return parts[3]
                return filepath

        # If we can't find it in the log pattern, look for the print line
        for entry in logs:
            if not entry:
                continue

            text = str(entry).strip()
            if text.startswith('s3a://') or ('finnhub/' in text and '.json' in text):
                return text.split('landing-zone/', 1)[-1] if 'landing-zone/' in text else text

        return None

    extract_filename = extract_output_filename()

    trigger_finnhub_processing = TriggerDagRunOperator(
        task_id="trigger_finnhub_processing",
        trigger_dag_id="finnhub_processing",
        conf={
            "logical_date": "{{ ds }}",
            "input_filename": "{{ task_instance.xcom_pull(task_ids='extract_output_filename') }}",
            "landing_zone_bucket": env_vars['MINIO_BUCKET']
        },
        wait_for_completion=False,
        poke_interval=30,
    )

    scrape_finnhub_news >> extract_filename >> trigger_finnhub_processing


# Instantiate the DAG
finnhub_crypto_news_scraper_dag_instance = finnhub_crypto_news_scraper_dag()
