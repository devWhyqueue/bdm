FROM maven:3.8.6-openjdk-11 AS builder

WORKDIR /app

# Copy the POM file
COPY pom.xml .

# Download dependencies
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the JAR
RUN mvn clean package -DskipTests

FROM apache/flink:1.19

# Copy the JAR file from the builder stage
COPY --from=builder /app/target/bitcoin-aggregator-*.jar /opt/flink/usrlib/bitcoin-aggregator.jar

# Copy the job submission script
COPY submit-job.sh /opt/flink/bin/

# Ensure the script uses Unix-style line endings and make the script executable
RUN sed -i 's/\r$//' /opt/flink/bin/submit-job.sh && \
    chmod +x /opt/flink/bin/submit-job.sh

# Set the entrypoint to the job submission script
ENTRYPOINT ["/opt/flink/bin/submit-job.sh"]
