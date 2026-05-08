FROM apache/spark:3.5.1


USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN if [ -s /tmp/requirements.txt ]; then \
    pip3 install --no-cache-dir -r /tmp/requirements.txt; \
    else \
    echo "No additional requirements."; \
    fi


WORKDIR /app
COPY spark_evaluation_job.py .



USER spark

CMD ["/opt/spark/bin/spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--driver-memory", "2g", \
    "--executor-memory", "2g", \
    "spark_evaluation_job.py"]