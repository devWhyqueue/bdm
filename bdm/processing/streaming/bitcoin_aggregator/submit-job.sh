#!/bin/bash

check_jobmanager() {
  echo "Checking JobManager availability..."
  for i in {1..12}; do
    if echo > /dev/tcp/jobmanager/8081; then
      echo "JobManager is available."
      return 0
    fi
    echo "JobManager not available, retrying in 10 seconds..."
    sleep 10
  done
  echo "JobManager is not available after multiple attempts. Exiting."
  exit 1
}

submit_flink_job() {
  echo "Submitting Bitcoin Aggregator job to Flink..."
  if flink run -m jobmanager:8081 -d /opt/flink/usrlib/bitcoin-aggregator.jar; then
    echo "Job submitted successfully."
  else
    echo "Failed to submit job to Flink."
    exit 1
  fi
}

# Main script execution
check_jobmanager
submit_flink_job
