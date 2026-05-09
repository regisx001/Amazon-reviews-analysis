FROM apache/spark:3.5.1

USER root

# ── Kafka JARs (explicit curl — identical to Dockerfile.spark) ───────────────
RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL -o /opt/spark/jars/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSL -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# ── Python dependencies ───────────────────────────────────────────────────────
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ── NLTK data (baked in — same path as preprocessing_job) ────────────────────
ENV NLTK_DATA=/usr/share/nltk_data
RUN python3 -m nltk.downloader -d /usr/share/nltk_data wordnet omw-1.4

# ── App ───────────────────────────────────────────────────────────────────────
COPY spark_streaming_predict_job.py /app/spark_streaming_predict_job.py
WORKDIR /app

ENV PATH="/opt/spark/bin:${PATH}"

USER spark

CMD ["spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--driver-memory", "2g", \
    "--executor-memory", "2g", \
    "spark_streaming_predict_job.py"]