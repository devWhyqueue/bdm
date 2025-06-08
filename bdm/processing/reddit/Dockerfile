FROM bitnami/spark:3.5.6

WORKDIR /app

# 0) Copy spark-defaults.conf into the Spark conf dir
COPY architecture/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# 1) Copy the entire `bdm` package into /app/bdm
COPY ./bdm /app/bdm

# 2) Copy and install this service’s Python deps
COPY bdm/processing/reddit/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 3) Make /app the import root, so `import bdm…` works
ENV PYTHONPATH=/app

# 4) Launch your Spark job as a module under /app/bdm
ENTRYPOINT ["spark-submit", "/app/bdm/processing/reddit/process_reddit_data.py"]

# 5) Allow Airflow (or you) to append extra flags
CMD []
