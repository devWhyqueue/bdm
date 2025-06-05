FROM flink:1.17-java11

WORKDIR /opt/flink-job

COPY target/finazon-stream-pipeline-processor.jar finazon-stream-pipeline-processor.jar

# Pass all parameters as arguments
ENTRYPOINT ["flink", "run", "-c", "com.bdm.FinazonStreamPipelineProcessor", "finazon-stream-pipeline-processor.jar"]
# Usage: docker run ... finazon-stream-pipeline-processor <price_ticks_topic> <volume_stream_topic> <kafka_bootstrap_servers> <influxdb_port>
