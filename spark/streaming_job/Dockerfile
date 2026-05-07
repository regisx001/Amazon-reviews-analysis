FROM apache/spark:3.5.1

USER root

# Download Kafka integration JARs at build time so executors have them
RUN /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --conf spark.jars.resolve.only=true \
    /dev/null 2>/dev/null || true && \
    cp /root/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/ && \
    cp /root/.ivy2/jars/org.apache.kafka_kafka-clients-3.4.1.jar /opt/spark/jars/ && \
    cp /root/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY spark_streaming_predict_job.py /app/spark_streaming_predict_job.py
WORKDIR /app

ENV PATH="/opt/spark/bin:${PATH}"

# No --packages needed at runtime since JARs are already in /opt/spark/jars/
CMD ["spark-submit", \
    "--master", "spark://spark-master:7077", \
    "spark_streaming_predict_job.py"]