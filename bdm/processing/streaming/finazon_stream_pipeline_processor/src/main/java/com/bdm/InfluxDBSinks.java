package com.bdm;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

public class InfluxDBSinks {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Sink for VWAP results
    public static class InfluxDBVWAPSink extends RichSinkFunction<String> {
        private transient InfluxDBClient influxDBClient;
        private transient WriteApi writeApi;
        private final String bucket;
        private final String org;
        private final String token;
        private final String url;

        public InfluxDBVWAPSink(String url, String bucket, String org, String token) {
            this.url = url;
            this.bucket = bucket;
            this.org = org;
            this.token = token;
        }

        @Override
        public void open(Configuration parameters) {
            influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray(), org, bucket);
            writeApi = influxDBClient.getWriteApi();
        }

        @Override
        public void invoke(String value, Context context) {
            try {
                JsonNode node = objectMapper.readTree(value);
                String symbol = node.get("symbol").asText();
                long windowEnd = node.get("window_end").asLong();
                double vwap = node.get("vwap").asDouble();
                Point point = Point.measurement("vwap_1min")
                        .addTag("symbol", symbol)
                        .addField("vwap", vwap)
                        .time(windowEnd, WritePrecision.MS);
                writeApi.writePoint(point);
            } catch (Exception e) {
                System.err.println("Failed to write VWAP to InfluxDB: " + e.getMessage());
            }
        }

        @Override
        public void close() {
            if (writeApi != null) writeApi.close();
            if (influxDBClient != null) influxDBClient.close();
        }
    }

    // Sink for hot-path (price tick) results
    public static class InfluxDBHotPathSink extends RichSinkFunction<String> {
        private transient InfluxDBClient influxDBClient;
        private transient WriteApi writeApi;
        private final String bucket;
        private final String org;
        private final String token;
        private final String url;

        public InfluxDBHotPathSink(String url, String bucket, String org, String token) {
            this.url = url;
            this.bucket = bucket;
            this.org = org;
            this.token = token;
        }

        @Override
        public void open(Configuration parameters) {
            influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray(), org, bucket);
            writeApi = influxDBClient.getWriteApi();
        }

        @Override
        public void invoke(String value, Context context) {
            try {
                JsonNode node = objectMapper.readTree(value);
                String symbol = node.get("symbol").asText();
                double price = node.get("price").asDouble();
                long timestamp = node.get("timestamp").asLong();
                long latency = node.has("latency_ms") ? node.get("latency_ms").asLong() : 0L;
                Point point = Point.measurement("hot_path_ticks")
                        .addTag("symbol", symbol)
                        .addField("price", price)
                        .addField("latency_ms", latency)
                        .time(timestamp, WritePrecision.MS);
                writeApi.writePoint(point);
            } catch (Exception e) {
                System.err.println("Failed to write hot-path tick to InfluxDB: " + e.getMessage());
            }
        }

        @Override
        public void close() {
            if (writeApi != null) writeApi.close();
            if (influxDBClient != null) influxDBClient.close();
        }
    }
}
