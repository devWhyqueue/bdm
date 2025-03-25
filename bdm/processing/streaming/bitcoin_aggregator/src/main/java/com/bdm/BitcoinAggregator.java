package com.bdm;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BitcoinAggregator {
    // POJO class for aggregation results
    public static class BitcoinAggregation {
        public String timestamp;
        public String windowStart;
        public String windowEnd;
        public double avgPrice;
        public double medianPrice;
        public double minPrice;
        public double maxPrice;
        
        public BitcoinAggregation() {}
        
        public BitcoinAggregation(String timestamp, String windowStart, String windowEnd, 
                                 double avgPrice, double medianPrice, double minPrice, 
                                 double maxPrice) {
            this.timestamp = timestamp;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.avgPrice = avgPrice;
            this.medianPrice = medianPrice;
            this.minPrice = minPrice;
            this.maxPrice = maxPrice;
        }
        
        @Override
        public String toString() {
            try {
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode json = mapper.createObjectNode();
                json.put("timestamp", timestamp);
                json.put("window_start", windowStart);
                json.put("window_end", windowEnd);
                json.put("avg_price", avgPrice);
                json.put("median_price", medianPrice);
                json.put("min_price", minPrice);
                json.put("max_price", maxPrice);
                return mapper.writeValueAsString(json);
            } catch (Exception e) {
                return "Error serializing to JSON: " + e.getMessage();
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Disable checkpointing to avoid checkpoint directory issues
        env.getCheckpointConfig().disableCheckpointing();
        
        // Use HashMap state backend (in-memory)
        env.setStateBackend(new HashMapStateBackend());
        
        // Get environment variables
        String kafkaBootstrapServers = System.getenv("KAFKA_ENDPOINT");
        String kafkaTopic = System.getenv("KAFKA_TOPIC");
        String minioEndpoint = System.getenv("MINIO_ENDPOINT");
        String minioAccessKey = System.getenv("MINIO_ACCESS_KEY");
        String minioSecretKey = System.getenv("MINIO_SECRET_KEY");
        String minioBucket = System.getenv("MINIO_BUCKET");
        
        // Configure Kafka consumer properties
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaBootstrapServers);
        kafkaProps.setProperty("group.id", "bitcoin-aggregator");
        kafkaProps.setProperty("auto.offset.reset", "latest");
        
        // Create Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            kafkaTopic,
            new SimpleStringSchema(),
            kafkaProps
        );
        
        // Create DataStream from Kafka consumer
        DataStream<String> stream = env.addSource(consumer);
        
        // Parse JSON and extract price
        DataStream<Double> prices = stream.flatMap(
                (FlatMapFunction<String, Double>) (value, out) -> {
                    try {
                        JsonNode jsonNode = new ObjectMapper().readTree(value);
                        if (jsonNode.has("p")) {
                            out.collect(jsonNode.get("p").asDouble());
                        }
                    } catch (Exception e) {
                        System.err.println("Error parsing JSON: " + e.getMessage());
                    }
                }).returns(Double.class);
        
        // Aggregate data in 1-minute windows
        DataStream<BitcoinAggregation> aggregations = prices
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .aggregate(new BitcoinPriceAggregator())
                .returns(BitcoinAggregation.class);
        
        // Use custom MinioSink to write directly to MinIO
        System.out.println("Writing output directly to MinIO bucket: " + minioBucket);
        aggregations.addSink(new MinioSink(
                minioEndpoint,
                minioAccessKey,
                minioSecretKey,
                minioBucket,
                "bitcoin-aggregations"
        ));
        
        // Execute the job
        env.execute("Bitcoin Price Aggregator");
    }
    
    // Aggregation function for Bitcoin prices
    private static class BitcoinPriceAggregator implements 
            AggregateFunction<Double, Tuple4<List<Double>, Double, Double, Double>, BitcoinAggregation> {
        
        @Override
        public Tuple4<List<Double>, Double, Double, Double> createAccumulator() {
            return new Tuple4<>(new ArrayList<>(), 0.0, Double.MAX_VALUE, Double.MIN_VALUE);
        }
        
        @Override
        public Tuple4<List<Double>, Double, Double, Double> add(
                Double price, Tuple4<List<Double>, Double, Double, Double> acc) {
            acc.f0.add(price);
            acc.f1 += price;
            acc.f2 = Math.min(acc.f2, price);
            acc.f3 = Math.max(acc.f3, price);
            return acc;
        }
        
        @Override
        public BitcoinAggregation getResult(Tuple4<List<Double>, Double, Double, Double> acc) {
            double avg = !acc.f0.isEmpty() ? acc.f1 / acc.f0.size() : 0.0;
            
            // Calculate median
            double median = 0.0;
            if (!acc.f0.isEmpty()) {
                Collections.sort(acc.f0);
                int middle = acc.f0.size() / 2;
                median = acc.f0.size() % 2 == 0 ? 
                        (acc.f0.get(middle - 1) + acc.f0.get(middle)) / 2.0 : 
                        acc.f0.get(middle);
            }
            
            // Get timestamps
            LocalDateTime now = LocalDateTime.now();
            String timestamp = now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            String windowEnd = timestamp;
            String windowStart = now.minusMinutes(1).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            
            return new BitcoinAggregation(
                    timestamp,
                    windowStart,
                    windowEnd,
                    avg,
                    median,
                    acc.f2 == Double.MAX_VALUE ? 0.0 : acc.f2,
                    acc.f3 == Double.MIN_VALUE ? 0.0 : acc.f3
            );
        }
        
        @Override
        public Tuple4<List<Double>, Double, Double, Double> merge(
                Tuple4<List<Double>, Double, Double, Double> a,
                Tuple4<List<Double>, Double, Double, Double> b) {
            a.f0.addAll(b.f0);
            a.f1 += b.f1;
            a.f2 = Math.min(a.f2, b.f2);
            a.f3 = Math.max(a.f3, b.f3);
            return a;
        }
    }
}
