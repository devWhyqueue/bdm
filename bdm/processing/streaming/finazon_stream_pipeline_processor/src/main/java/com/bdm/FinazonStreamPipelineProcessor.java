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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

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
                            if (node.has("timestamp")) {
                                return node.get("timestamp").asLong();
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
                double price = node.has("price") ? node.get("price").asDouble() : -1;
                if (price < 0) {
                    System.err.println("Invalid price tick (p < 0): " + value);
                    return; // skip invalid
                }
                // Latency measurement
                long eventTimestamp = node.has("timestamp") ? node.get("timestamp").asLong() : 0L;
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

    // 1. Preprocess warm path: compute per-second volume
    public static class PerSecondVolume extends RichFlatMapFunction<String, String> {
        private transient ValueState<Long> prevVolumeState;
        private transient ValueState<Long> prevTimestampState;

        @Override
        public void open(Configuration parameters) {
            prevVolumeState = getRuntimeContext().getState(new ValueStateDescriptor<>("prevVolume", Long.class));
            prevTimestampState = getRuntimeContext().getState(new ValueStateDescriptor<>("prevTimestamp", Long.class));
        }

        @Override
        public void flatMap(String value, Collector<String> out) {
            try {
                JsonNode node = objectMapper.readTree(value);
                long currVolume = node.get("volume").asLong();
                long currTimestamp = node.get("timestamp").asLong();
                long prevVolume = prevVolumeState.value() == null ? 0L : prevVolumeState.value();
                long prevTimestamp = prevTimestampState.value() == null ? 0L : prevTimestampState.value();
                long perSecondVolume = currVolume;
                if (currVolume >= prevVolume && currTimestamp > prevTimestamp) {
                    perSecondVolume = currVolume - prevVolume;
                } else if (currVolume < prevVolume) {
                    // Reset finazon aggregation (new minute)
                    perSecondVolume = currVolume;
                }
                ((com.fasterxml.jackson.databind.node.ObjectNode) node).put("per_second_volume", perSecondVolume);
                out.collect(node.toString());
                prevVolumeState.update(currVolume);
                prevTimestampState.update(currTimestamp);
            } catch (Exception e) {
                System.err.println("Failed to process volume tick: " + value);
            }
        }
    }

    // 2. Join function for VWAP calculation
    public static class VWAPJoinFunction extends ProcessJoinFunction<String, String, String> {
        @Override
        public void processElement(String priceJson, String volumeJson, Context ctx, Collector<String> out) {
            try {
                JsonNode priceNode = objectMapper.readTree(priceJson);
                JsonNode volumeNode = objectMapper.readTree(volumeJson);
                double price = priceNode.has("price") ? priceNode.get("price").asDouble() : 0.0;
                long perSecondVolume = volumeNode.has("per_second_volume") ? volumeNode.get("per_second_volume").asLong() : 0L;
                String symbol = priceNode.has("symbol") ? priceNode.get("symbol").asText() : "unknown";
                long timestamp = volumeNode.has("timestamp") ? volumeNode.get("timestamp").asLong() : 0L;
                // Output tuple for window aggregation
                out.collect(String.format("{\"symbol\":\"%s\",\"timestamp\":%d,\"price\":%f,\"per_second_volume\":%d}", symbol, timestamp, price, perSecondVolume));
            } catch (Exception e) {
                System.err.println("Failed to join price and volume: " + e.getMessage());
            }
        }
    }

    // 3. VWAP window aggregation
    public static class VWAPWindowFunction implements WindowFunction<String, String, String, TimeWindow> {
        @Override
        public void apply(String symbol, TimeWindow window, Iterable<String> input, Collector<String> out) {
            double sumPxV = 0.0;
            long sumV = 0L;
            long windowEnd = window.getEnd();
            for (String json : input) {
                try {
                    JsonNode node = objectMapper.readTree(json);
                    double price = node.has("price") ? node.get("price").asDouble() : 0.0;
                    long perSecondVolume = node.has("per_second_volume") ? node.get("per_second_volume").asLong() : 0L;
                    sumPxV += price * perSecondVolume;
                    sumV += perSecondVolume;
                } catch (Exception e) {
                    System.err.println("Failed to process input for VWAP: " + e.getMessage());
                }
            }
            double vwap = sumV > 0 ? sumPxV / sumV : 0.0;
            out.collect(String.format("{\"symbol\":\"%s\",\"window_end\":%d,\"vwap\":%f}", symbol, windowEnd, vwap));
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
        // validatedHotStream.print(); // For debugging, print to console

        // Warm path (full market data)
        FlinkKafkaConsumer<String> warmConsumer = new FlinkKafkaConsumer<>(
                volumeStreamTopic,
                new SimpleStringSchema(),
                properties
        );
        warmConsumer.assignTimestampsAndWatermarks(eventTimeWatermarkStrategy());
        DataStream<String> warmStream = env.addSource(warmConsumer).name("Warm Path - Full Market Data");
        // warmStream.print(); // For debugging, print to console

        // Preprocess warm path to compute per-second volume
        DataStream<String> warmStreamPerSecond = warmStream
            .keyBy(json -> {
                try {
                    return objectMapper.readTree(json).get("symbol").asText();
                } catch (Exception e) {
                    return "unknown";
                }
            })
            .flatMap(new PerSecondVolume())
            .name("Per-Second Volume (Warm Path)");

        // Key both streams by symbol
        KeyedStream<String, String> keyedHot = validatedHotStream.keyBy(json -> {
            try {
                return objectMapper.readTree(json).get("symbol").asText();
            } catch (Exception e) {
                return "unknown";
            }
        });
        KeyedStream<String, String> keyedWarm = warmStreamPerSecond.keyBy(json -> {
            try {
                return objectMapper.readTree(json).get("symbol").asText();
            } catch (Exception e) {
                return "unknown";
            }
        });

        // Interval join in 1min window: join price ticks with per-second volume
        DataStream<String> joined = keyedHot
            .intervalJoin(keyedWarm)
            .between(Time.seconds(-1), Time.seconds(1)) // join price and volume ticks within 1s
            .process(new VWAPJoinFunction())
            .name("Joined Price-Volume");

        // Windowed VWAP calculation: 1min tumbling window
        DataStream<String> vwapStream = joined
            .keyBy(json -> {
                try {
                    return objectMapper.readTree(json).get("symbol").asText();
                } catch (Exception e) {
                    return "unknown";
                }
            })
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .apply(new VWAPWindowFunction())
            .name("VWAP 1min Window");

        vwapStream.print(); // Output VWAP to console (replace with InfluxDB sink as needed)

        env.execute("Finazon Stream Pipeline Processor");
    }
}
