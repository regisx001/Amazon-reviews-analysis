# Use the same Spark version as your cluster
FROM apache/spark:3.5.1

# Switch to root to install additional dependencies (if any)
USER root

# Copy requirements first (for better caching)
COPY requirements.txt /tmp/requirements.txt

# Install Python packages (if requirements.txt is not empty)
RUN if [ -s /tmp/requirements.txt ]; then \
    pip install --no-cache-dir -r /tmp/requirements.txt; \
    else \
    echo "No additional requirements."; \
    fi

# Copy the preprocessing script into the image
COPY spark_preprocessing_job.py /app/spark_preprocessing_job.py

# Change working directory
WORKDIR /app

# Switch to the non‑root user (optional but good practice)
USER spark

# Default command: submit the job to the Spark master
# Override the master URL via --master if needed
CMD ["/opt/spark/bin/spark-submit", \
    "--master", "spark://spark-master:7077", \
    "spark_preprocessing_job.py"]