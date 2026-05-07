FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python

COPY test_data_kafka_producer.py /app/test_data_kafka_producer.py

CMD ["python", "/app/test_data_kafka_producer.py"]
