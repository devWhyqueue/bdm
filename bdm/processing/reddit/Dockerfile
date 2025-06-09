FROM bitnami/spark:3.5.6
WORKDIR /app

USER root

# 0) Create a real spark user with UID=1001 and home directory
# 1) Install FFmpeg & ImageMagick and prepare Ivy cache dir
RUN useradd -m -u 1001 -s /bin/bash spark && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      ffmpeg \
      imagemagick \
      libmagic1 && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    mkdir -p /tmp/.ivy2 && \
    chown spark:spark /tmp/.ivy2

# 3) Switch to the newly created spark user
USER spark
ENV HOME=/home/spark

# 4) Copy configs and install Python deps
COPY architecture/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY ./bdm /app/bdm
COPY bdm/processing/reddit/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 5) Make /app importable
ENV PYTHONPATH=/app

# 6) Launch Spark with Ivy cache set
ENTRYPOINT ["spark-submit", "--conf", "spark.jars.ivy=/tmp/.ivy2", "/app/bdm/processing/reddit/spark_processing/process_reddit_data.py"]
CMD []
