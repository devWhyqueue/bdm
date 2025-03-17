import logging
from datetime import datetime, timedelta
from typing import List, Dict

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from confluent_kafka import Producer, KafkaException

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    description='Test connections to Kafka and S3 (MinIO)',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 3, 16),
    catchup=False,
    tags=['test', 'kafka', 's3', 'minio']
)
def test_connections() -> None:
    @task
    def test_s3_connection() -> str:
        """Test connection to S3 (MinIO)."""
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_client = s3_hook.get_conn()
        buckets: List[str] = [b["Name"] for b in s3_client.list_buckets().get("Buckets", [])]

        test_content = "This is a test file created by Airflow"
        test_key = "test_file.txt"

        s3_hook.load_string(test_content, key=test_key, bucket_name="airflow")
        file_exists = s3_hook.check_for_key(test_key, bucket_name="airflow")
        if file_exists:
            s3_hook.delete_objects(bucket="airflow", keys=[test_key])

        return f"S3 connection successful. Buckets: {buckets}. Test file created and deleted."

    @task
    def test_kafka_connection() -> str:
        """Test connection to a Kafka broker using confluent_kafka."""
        kafka_config = {
            "bootstrap.servers": "kafka:9092",
            "client.id": "airflow-test-client",
            "message.timeout.ms": 5000,  # 5-second timeout
        }
        delivery_errors: List[Exception] = []

        def delivery_report(err: KafkaException, msg: Producer) -> None:
            if err is not None:
                logging.error("Message delivery failed: %s", err)
                delivery_errors.append(err)
            else:
                logging.info("Message delivered to %s [partition %d]", msg.topic(), msg.partition())

        try:
            producer = Producer(kafka_config)
            topic = "airflow-test-topic"
            message = "This is a test message from Airflow".encode("utf-8")

            producer.produce(topic, value=message, callback=delivery_report)
            producer.flush(timeout=10)  # Wait for delivery up to 10 seconds

            if delivery_errors:
                raise RuntimeError("Kafka message delivery failed. See log for details.")

            logging.info("Kafka connection successful. Test message sent to %s.", topic)
            return f"Kafka connection successful. Test message sent to {topic}."
        except (KafkaException, Exception) as e:
            logging.error("Kafka connection failed: %s", e)
            raise

    @task
    def process_results(s3_res: str, kafka_res: str) -> Dict[str, str]:
        """Log and return the results of the connection tests."""
        logging.info("S3 (MinIO): %s", s3_res)
        logging.info("Kafka: %s", kafka_res)
        return {
            "s3": s3_res,
            "kafka": kafka_res,
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }

    s3_result = test_s3_connection()
    kafka_result = test_kafka_connection()
    process_results(s3_result, kafka_result)


test_connections_dag = test_connections()
