package com.bdm;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.time.Duration;
import java.util.Properties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FinazonStreamPipelineProcessor {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Use event time watermark strategy for both hot and warm paths
    private static WatermarkStrategy<String> eventTimeWatermarkStrategy() {
        return WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        try {
                            JsonNode node = objectMapper.readTree(element);
                            // Try 'timestamp' (hot path) or 't' (warm path)
                            if (node.has("timestamp")) {
                                return node.get("timestamp").asLong() * 1000L;
                            } else if (node.has("t")) {
                                return node.get("t").asLong() * 1000L;
                            }
                        } catch (Exception e) {
                            // fallback: use current system time
                            return System.currentTimeMillis();
                        }
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: <price_ticks_topic> <volume_stream_topic> <kafka_bootstrap_servers> <influxdb_port>");
            System.exit(1);
        }
        String priceTicksTopic = args[0];
        String volumeStreamTopic = args[1];
        String kafkaBootstrapServers = args[2];
        String influxdbPort = args[3];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBootstrapServers);
        properties.setProperty("group.id", "finazon-stream-pipeline-processor");

        // Hot path (price ticks)
        FlinkKafkaConsumer<String> hotConsumer = new FlinkKafkaConsumer<>(
                priceTicksTopic,
                new SimpleStringSchema(),
                properties
        );
        hotConsumer.assignTimestampsAndWatermarks(eventTimeWatermarkStrategy());
        DataStream<String> hotStream = env.addSource(hotConsumer).name("Hot Path - Price Ticks");

        // Warm path (full market data)
        FlinkKafkaConsumer<String> warmConsumer = new FlinkKafkaConsumer<>(
                volumeStreamTopic,
                new SimpleStringSchema(),
                properties
        );
        warmConsumer.assignTimestampsAndWatermarks(eventTimeWatermarkStrategy());
        DataStream<String> warmStream = env.addSource(warmConsumer).name("Warm Path - Full Market Data");

        // TODO: Implement stream join, latency measurement, and VWAP calculation
        // TODO: Output results to InfluxDB sink (using influxdbPort)

        env.execute("Finazon Stream Pipeline Processor");
    }
}
