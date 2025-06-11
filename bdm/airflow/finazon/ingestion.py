from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.docker.hooks.docker import DockerHook

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    default_args=default_args,
    description='Orchestrate WebSocket streaming finnhub for asset data',
    schedule_interval='@hourly',
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=['streaming', 'websocket', 'kafka', 'asset_data'],
)
def finazon_asset_ticks():
    """DAG for monitoring WebSocket service and ingesting asset data."""

    @task
    def monitor_service() -> None:
        """Monitor the WebSocket service to ensure it's operating correctly."""
        hook = DockerHook(None, base_url='unix://var/run/docker.sock')
        client = hook.get_conn()

        containers = client.containers(all=True)
        container = next((c for c in containers if 'finazon_stream_ingestion' in c['Image']), None)
        container_status = container['State']

        if container_status != 'running':
            raise ValueError(f"WebSocket service is not running. Status: {container_status}")

    monitor_service()


finazon_asset_ticks()
