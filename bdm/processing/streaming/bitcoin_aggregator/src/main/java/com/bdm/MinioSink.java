package com.bdm;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * A custom sink that writes data directly to MinIO using the AWS SDK.
 */
public class MinioSink extends RichSinkFunction<BitcoinAggregator.BitcoinAggregation> {
    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String bucket;
    private final String prefix;
    
    private transient S3Client s3Client;
    private transient DateTimeFormatter dateFormatter;
    
    public MinioSink(String endpoint, String accessKey, String secretKey, String bucket, String prefix) {
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
        this.prefix = prefix;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // Initialize S3 client with MinIO configuration
        String endpointUrl = endpoint.startsWith("http") ? endpoint : "http://" + endpoint;
        System.out.println("Connecting to MinIO at: " + endpointUrl);
        
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(endpointUrl))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.US_EAST_1) // MinIO requires a region, but it doesn't matter which one
                .httpClient(UrlConnectionHttpClient.builder().build())
                .serviceConfiguration(s -> s.pathStyleAccessEnabled(true)) // Enable path-style access for MinIO
                .build();
        
        dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    }
    
    @Override
    public void invoke(BitcoinAggregator.BitcoinAggregation value, Context context) throws Exception {
        // Create a key with date-based partitioning
        String date = LocalDateTime.now().format(dateFormatter);
        String key = String.format("%s/%s/bitcoin-aggregation-%s.json", 
                prefix, date, System.currentTimeMillis());
        
        // Convert the aggregation to JSON
        String json = value.toString();
        
        // Upload to MinIO
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/json")
                .build();
        
        s3Client.putObject(putObjectRequest, RequestBody.fromString(json));
    }
    
    @Override
    public void close() throws Exception {
        if (s3Client != null) {
            s3Client.close();
        }
    }
}
