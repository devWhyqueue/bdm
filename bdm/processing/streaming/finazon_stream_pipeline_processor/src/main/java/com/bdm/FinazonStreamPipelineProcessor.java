package com.bdm;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
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
                            // Use 't' for timestamp
                            if (node.has("t")) {
                                return node.get("t").asLong();
                            }
                        } catch (Exception e) {
                            // fallback: use current system time
                            return System.currentTimeMillis();
                        }
                        return System.currentTimeMillis();
                    }
                });
    }

    public static class PriceTickValidatorAndLatency implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) {
            try {
                JsonNode node = objectMapper.readTree(value);
                // Constraint validation: p >= 0
                double price = node.has("p") ? node.get("p").asDouble() : -1;
                if (price < 0) {
                    System.err.println("Invalid price tick (p < 0): " + value);
                    return; // skip invalid
                }
                // Latency measurement
                long eventTimestamp = node.has("t") ? node.get("t").asLong() : 0L;
                long nowUtc = java.time.Instant.now().toEpochMilli();
                long latencyMs = nowUtc - eventTimestamp;
                // If latency is negative, set to 0. This can happen due to unsynced clocks between containers.
                if (latencyMs < 0) {
                    latencyMs = 0;
                }
                // Add latency and comparison time to the output JSON
                ((com.fasterxml.jackson.databind.node.ObjectNode) node).put("latency_ms", latencyMs);
                out.collect(node.toString());
            } catch (Exception e) {
                System.err.println("Failed to process price tick: " + value);
            }
        }
    }

    public static String getEnvOrThrow(String key) {
        String value = System.getenv(key);
        if (value == null || value.isEmpty()) {
            System.err.println("Missing required environment variable: " + key);
            System.exit(1);
        }
        return value;
    }

    public static void main(String[] args) throws Exception {
        String priceTicksTopic = getEnvOrThrow("PRICE_TICKS_TOPIC");
        String volumeStreamTopic = getEnvOrThrow("VOLUME_STREAM_TOPIC");
        String kafkaBootstrapServers = getEnvOrThrow("KAFKA_ENDPOINT");
        String influxdbPort = getEnvOrThrow("INFLUXDB_PORT");

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

        // Hot path: validate and measure latency
        DataStream<String> validatedHotStream = hotStream.flatMap(new PriceTickValidatorAndLatency())
            .name("Validate & Measure Latency (Hot Path)");
        validatedHotStream.print(); // For debugging, print to console

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
