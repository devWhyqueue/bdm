from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


def create_scraper_task(dag, subreddit, sort_by='new', time_filter='all',
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
        dag=dag,
    )


# Define the DAG
subreddit_scraper = DAG(
    'subreddit_scraper',
    default_args=default_args,
    description='Scrape various subreddits',
    schedule_interval='* * * * *',  # Run every minute
    start_date=datetime(2025, 3, 22),
    catchup=False,
)

# Bitcoin - new posts every minute
bitcoin_task = create_scraper_task(
    dag=subreddit_scraper,
    subreddit='bitcoin',
    sort_by='new',
    limit=5,
)
