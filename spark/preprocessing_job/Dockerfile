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

RUN pip3 install --no-cache-dir nltk

ENV NLTK_DATA=/opt/nltk_data

RUN mkdir -p /opt/nltk_data
RUN python3 -m nltk.downloader -d /opt/nltk_data wordnet
RUN python3 -m nltk.downloader -d /opt/nltk_data omw-1.4

WORKDIR /app
COPY spark_preprocessing_job.py .

USER spark

CMD ["/opt/spark/bin/spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--driver-memory", "4g", \
    "--executor-memory", "4g", \
    "spark_preprocessing_job.py"]