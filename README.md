# Amazon Reviews Real-Time Sentiment Analysis Platform

**Scalable, production-grade system for real-time sentiment analysis of Amazon customer reviews with streaming data pipelines, ML model predictions, and interactive analytics dashboards.**

---

## Table of Contents
1. [Global Overview](#global-overview)
2. [Core Technologies & Stack](#core-technologies--stack)
3. [Architecture & Pipeline](#architecture--pipeline)
4. [Project Structure](#project-structure)
5. [How to Run](#how-to-run)
6. [Features & Usage](#features--usage)
7. [API Documentation](#api-documentation)
8. [Monitoring & Debugging](#monitoring--debugging)
9. [Development Notes](#development-notes)

---

## Global Overview

### What This Project Does

This platform ingests Amazon customer review data in real-time, applies machine learning to predict sentiment (positive/neutral/negative), aggregates predictions into dashboards, and monitors model performance for data drift. It's built for scalability with containerized microservices, distributed data processing, and event-driven architectures.

### Why It Exists

Traditional batch analytics cannot capture the velocity of customer feedback. This system enables:
- **Real-time insight** into customer sentiment trends as reviews flow in
- **Automated ML pipeline** that predicts sentiment at scale using PySpark
- **Data-driven decision making** with pre-computed aggregations optimized for speed
- **Production monitoring** with drift detection to alert on model degradation

---

## Core Technologies & Stack

| Layer | Technologies |
|-------|--------------|
| **Data Streaming** | Apache Kafka, Zookeeper, Confluent |
| **Data Processing** | Apache Spark (3.5.1), PySpark |
| **Orchestration** | Apache Airflow |
| **Backend** | FastAPI, Python 3.10+ |
| **Database** | MongoDB, MongoDB Express |
| **Frontend** | Svelte 5, TypeScript, Vite, Chart.js |
| **ML/AI** | Logistic Regression, scikit-learn, NLTK |
| **Containerization** | Docker, Docker Compose |
| **Infrastructure** | Docker networks, volume persistence |

---

## Architecture & Pipeline

### High-Level Data Flow

```
Raw Reviews (CSV)
    ↓
[Kafka Producer] → Kafka Topic (reviews.raw)
    ↓
[Spark Streaming Job] → Real-time Predictions
    ↓
MongoDB (predictions collection)
    ↓
[Airflow DAGs]
├── Aggregation DAG (nightly) → Pre-computed stats
├── Model Evaluation DAG (weekly) → Drift detection
└── Producer DAG (hourly) → Batch ingestion
    ↓
MongoDB (agg_monthly_sentiments, product_scoring, drift_status, model_insights)
    ↓
[FastAPI Backend] → REST Endpoints + Server-Sent Events
    ↓
[Svelte Dashboard] → Real-time Analytics & Visualizations
```

### Step-by-Step Pipeline Logic

#### **1. Data Ingestion & Preprocessing**
- **Source**: CSV file with ~500K Amazon reviews (from Kaggle dataset)
- **Spark Preprocessing Job**: 
  - Reads raw CSV into Spark DataFrame
  - Creates `Sentiment` column based on review score (< 3 = negative, == 3 = neutral, > 3 = positive)
  - Performs stratified split: 80% train, 10% validation, 10% test
  - Preserves class distribution across splits
  - Saves splits as CSV files for model training

#### **2. Kafka Stream Setup**
- **Kafka Producer DAG** (runs hourly via Airflow):
  - Reads test CSV batches from disk
  - Publishes reviews as JSON messages to Kafka topic `reviews.raw`
  - Logs statistics to MongoDB for audit trail
  
- **Kafka Cluster**: 
  - 1 broker, Zookeeper coordination
  - Kafka UI available for monitoring messages

#### **3. Real-Time Sentiment Prediction (Spark Streaming)**
- **Spark Streaming Job**:
  - Consumes messages from Kafka topic `reviews.raw`
  - Applies pre-trained Logistic Regression model for sentiment classification
  - Extracts features: text tokenization → stop word removal → TF-IDF vectorization
  - Predicts sentiment class: 0 (negative), 1 (neutral), 2 (positive)
  - Enriches record with timestamp and prediction confidence
  - **Writes predictions to MongoDB** `predictions` collection in batches (1000 records/batch)

#### **4. Nightly Aggregation (Airflow)**
- **Monthly Aggregation Task**:
  - Groups all predictions by month and sentiment
  - Pre-computes counts for fast dashboard queries
  - Stores in `agg_monthly_sentiments` collection
  
- **Product Scoring Aggregation Task**:
  - Per-ProductId sentiment breakdown
  - Aggregates counts and percentages
  - Stores in `product_scoring` collection

#### **5. Weekly Model Evaluation (Airflow)**
- **Drift Detection Task**:
  - Computes current prediction distribution (% positive, neutral, negative)
  - Compares against thresholds:
    - Alert if neutral > 30% (indicates model may be underfitting)
    - Alert if negative > 40% (unusual spike in negative predictions)
  - If triggers exceed limits, logs warning to `drift_status` collection

#### **6. API & Dashboard**
- **FastAPI Backend** (`api/app/main.py`):
  - **Analytics endpoints**: Overall stats, trends by month, top products
  - **Predictions endpoints**: Recent predictions, product-specific scoring
  - **Streaming endpoints**: Server-Sent Events (SSE) for real-time dashboard updates
  - Reads from pre-aggregated collections for performance
  
- **Svelte Frontend**:
  - Real-time dashboards with interactive charts (Chart.js)
  - Sentiment trends over time (bar chart)
  - Top products by volume (bar chart)
  - Product sentiment breakdown (pie chart)
  - Drift status alerts
  - Aggregation status info
  - Auto-refresh via EventSource (SSE)

---

## Project Structure

### Visual Folder Tree

```
Amazon-reviews-analysis/
├── .env.example                              # Template for environment variables
├── docker-compose.yaml                       # Complete stack orchestration
├── requirements.txt                          # Root Python dependencies (notebooks)
├── data.txt                                  # Data source URL reference
│
├── api/                                      # FastAPI Backend
│   ├── Dockerfile                           # API container image
│   ├── main.py                              # FastAPI entry point
│   ├── requirements.txt                     # API Python dependencies
│   └── app/
│       ├── __init__.py
│       ├── main.py                          # FastAPI app factory
│       ├── core/
│       │   ├── config.py                    # Configuration & environment loading
│       │   └── database.py                  # MongoDB connection pool
│       ├── api/
│       │   └── v1/
│       │       ├── analytics.py             # Analytics endpoints (/api/stats, /api/predictions-by-date, etc)
│       │       ├── predictions.py           # Predictions endpoints (/api/recent-predictions, /api/product-scoring)
│       │       └── stream.py                # Server-Sent Events endpoint (/api/stream/stats)
│       ├── services/
│       │   ├── analytics_service.py         # Query logic for analytics
│       │   └── prediction_service.py        # Query logic for predictions
│       ├── schemas/
│       │   ├── analytics.py                 # Pydantic models for analytics responses
│       │   └── predictions.py               # Pydantic models for prediction responses
│       └── routers/
│           └── pages.py                     # Page routers (static HTML)
│
├── frontend/                                 # Svelte TypeScript Dashboard
│   ├── Dockerfile                           # Frontend container image
│   ├── package.json                         # Node dependencies & build scripts
│   ├── tsconfig.json                        # TypeScript configuration
│   ├── vite.config.ts                       # Vite build configuration
│   ├── svelte.config.js                     # Svelte configuration
│   ├── src/
│   │   ├── app.html                         # HTML entry point
│   │   ├── app.css                          # Global styles
│   │   ├── lib/
│   │   │   ├── api.ts                       # API client (fetch wrapper)
│   │   │   └── assets/                      # Images, icons, static files
│   │   └── routes/
│   │       ├── +page.svelte                 # Root route (redirects to /dashboard)
│   │       └── dashboard/
│   │           ├── +page.svelte             # Main dashboard
│   │           ├── [productId]/             # Product detail view
│   │           ├── models/                  # Model metrics view
│   │           ├── monthly/                 # Monthly trends view
│   │           └── recent/                  # Recent predictions view
│
├── kafka/                                    # Kafka Producer Service
│   ├── Dockerfile                           # Kafka producer container
│   └── test_data_kafka_producer.py          # Producer script (publishes to Kafka)
│
├── spark/                                    # Spark Jobs (Preprocessing & Streaming)
│   ├── Dockerfile.spark                     # Base Spark image
│   ├── preprocessing_job/
│   │   ├── Dockerfile                       # Preprocessing job container
│   │   ├── requirements.txt                 # Spark job Python dependencies
│   │   └── spark_preprocessing_job.py       # Stratified train/val/test split
│   └── streaming_job/
│       ├── Dockerfile                       # Streaming job container
│       ├── requirements.txt                 # Streaming job dependencies
│       └── spark_streaming_predict_job.py   # Real-time sentiment prediction via Kafka
│
├── airflow/                                  # Apache Airflow Orchestration
│   ├── Dockerfile.airflow                   # Airflow container
│   ├── config/
│   │   └── airflow.cfg                      # Airflow configuration
│   └── dags/
│       ├── kafka_producer_dag.py            # Hourly: Batch reviews to Kafka
│       ├── aggregate_dashboard_dag.py       # Nightly: Pre-compute analytics
│       ├── model_evaluation_dag.py          # Weekly: Drift detection
│       └── test_dag.py                      # Test DAG
│
├── notebooks/                                # Jupyter ML Experimentation
│   ├── 01-explore-data.ipynb                # EDA & data profiling
│   ├── 02-preprocessing.ipynb               # Feature engineering
│   ├── 03-models-experimenting.ipynb        # Model comparison
│   ├── 04-train-logistic-regresion-model.ipynb  # Final model training
│   └── 05-evaluate-model.ipynb              # Model evaluation metrics
│
├── research/                                 # Research & experimentation copies
│   └── [same structure as notebooks/]
│
├── data/                                     # Data directory (volumes mounted)
│   ├── data.txt                             # Raw reviews CSV
│   ├── train/                               # Training split
│   ├── val/                                 # Validation split
│   └── test/                                # Test split
│
└── PERMISSION.md                            # Project permissions & licensing
```

### Key Files Explained

| File | Purpose |
|------|---------|
| `docker-compose.yaml` | Defines all services (Spark, Kafka, MongoDB, Airflow, API, Frontend) and their networking |
| `api/app/main.py` | FastAPI app factory; mounts routers for analytics, predictions, streaming |
| `api/app/core/config.py` | Loads environment variables; configures MongoDB connection strings |
| `spark/streaming_job/spark_streaming_predict_job.py` | Consumes Kafka → applies ML model → writes to MongoDB |
| `airflow/dags/aggregate_dashboard_dag.py` | Nightly task: pre-computes aggregations for dashboard performance |
| `airflow/dags/model_evaluation_dag.py` | Weekly task: monitors prediction distribution for model drift |
| `frontend/src/routes/dashboard/+page.svelte` | Main dashboard component; renders charts, fetches data via API |
| `frontend/src/lib/api.ts` | API client; wraps fetch calls to backend endpoints |

---

## How to Run

### Prerequisites

Before starting, ensure you have:

- **Docker & Docker Compose**: [Install](https://docs.docker.com/compose/install/)
  - Test: `docker --version && docker-compose --version`
  
- **Python 3.10+**: Required for notebooks and local development
  - Test: `python --version`
  
- **Node.js 18+** (if developing frontend locally)
  - Test: `node --version && npm --version`

- **At least 8GB RAM** for Docker to comfortably run all services
- **~10GB disk space** for data volumes and container images

### Step 1: Clone & Setup

```bash
# Clone the repository
git clone <repository-url>
cd Amazon-reviews-analysis

# Copy environment template
cp .env.example .env

# Create data directories
mkdir -p data/train data/val data/test
```

### Step 2: Configure Environment

Edit `.env` with your settings (defaults work for local development):

```bash
# MongoDB
MONGODB_ROOT_USER=admin
MONGODB_ROOT_PASSWORD=admin123
MONGODB_DATABASE=amazon_reviews
MONGODB_HOST=mongodb
MONGODB_PORT=27017

# Kafka
KAFKA_BOOTSTRAP=kafka:9092
KAFKA_TOPIC=reviews.raw

# Airflow
AIRFLOW_HOME=/opt/airflow

# Frontend
FRONTEND_ORIGINS=http://localhost:5173,http://localhost:3000

# API
TARGET_PRODUCT=B00IGGY4K2
```

### Step 3: Prepare Data

The project expects a CSV file at `data/data.txt` or `data/reviews.csv`.

**Option A: Download from Kaggle** (if you have the dataset locally):
```bash
# Place your CSV at:
cp /path/to/your/amazon_reviews.csv data/reviews.csv
```

**Option B: Use sample data** (included in the repo):
The project will use test CSV files from `data/test/` for the Kafka producer.

### Step 4: Start All Services (Docker Compose)

```bash
# Start the entire stack
docker-compose up -d

# Monitor logs
docker-compose logs -f

# Or follow a specific service
docker-compose logs -f kafka
docker-compose logs -f spark-master
```

This will:
1. ✅ Start MongoDB (port `27017`)
2. ✅ Start Mongo Express UI (port `9090`)
3. ✅ Start Zookeeper & Kafka (ports `2181`, `9092`)
4. ✅ Start Kafka UI (port `8090`)
5. ✅ Start Spark master & workers (ports `8080`, `8081`, `8082`)
6. ✅ Start Airflow (port `8888`)
7. ✅ Start FastAPI backend (port `8000`)
8. ✅ Start Svelte frontend (port `5173`)

### Step 5: Run Preprocessing (One-Time Setup)

Preprocess the data into train/val/test splits:

```bash
# Start preprocessing job
docker-compose --profile preprocessing up preprocessing-job

# Monitor progress
docker-compose logs -f preprocessing-job

# Once complete, verify splits were created
docker exec preprocessing-job ls -lh /opt/spark/work-dir/data/{train,val,test}
```

### Step 6: Start Data Streaming

Trigger the Kafka producer and Spark streaming jobs:

```bash
# Start Kafka producer (runs in background)
docker-compose --profile producer up reviews-producer

# In another terminal, start Spark streaming job
docker-compose --profile streaming up streaming-job

# Watch for predictions being written to MongoDB
docker-compose logs -f streaming-job
```

### Step 7: Access the Dashboard

| Service | URL |
|---------|-----|
| **Dashboard** | http://localhost:5173 |
| **API Docs (Swagger)** | http://localhost:8000/docs |
| **MongoDB Express** | http://localhost:9090 (admin/admin123) |
| **Kafka UI** | http://localhost:8090 |
| **Spark Master UI** | http://localhost:8080 |
| **Airflow UI** | http://localhost:8888 |

### Step 8: Trigger Airflow DAGs (Optional)

Enable scheduled DAGs in Airflow:

```bash
# Access Airflow UI
# → Open http://localhost:8888
# → Click "DAGs" tab
# → Toggle "amazon_reviews_producer" to ON
# → Toggle "dashboard_aggregation" to ON
# → Toggle "model_evaluation" to ON
```

Or trigger manually:
```bash
docker-compose exec airflow-scheduler airflow dags trigger amazon_reviews_producer
docker-compose exec airflow-scheduler airflow dags trigger dashboard_aggregation
```

### Stopping Everything

```bash
# Stop all services
docker-compose down

# Remove volumes (caution: deletes data)
docker-compose down -v
```

---

## Features & Usage

### Core Features

#### 1. **Real-Time Dashboard**
- **Sentiment Statistics**: Live counts of positive, neutral, negative predictions
- **Monthly Trends**: Time-series chart of sentiment distribution over months
- **Top Products**: Bar chart of products by review volume
- **Product Breakdown**: Pie chart of sentiment percentages per product
- **Auto-Refresh**: Server-Sent Events (SSE) push real-time updates every second
- **Drift Alerts**: Visual indicator if model prediction distribution is degrading

#### 2. **Product Analytics**
Navigate to **Dashboard → Product Detail** (`/dashboard/[productId]`):
- Sentiment breakdown for a specific product
- Customer score vs. predicted sentiment comparison
- Recent customer reviews for that product

#### 3. **Model Metrics**
Navigate to **Dashboard → Models** (`/dashboard/models`):
- Confusion matrix from latest model evaluation
- Precision, recall, F1 metrics per sentiment class
- Model training metadata (version, timestamp, accuracy)

#### 4. **Recent Predictions**
Navigate to **Dashboard → Recent** (`/dashboard/recent`):
- Stream of latest predictions (most recent first)
- Shows: Review ID, Product ID, Customer profile, Predicted sentiment, Actual score

#### 5. **Monthly Trends**
Navigate to **Dashboard → Monthly** (`/dashboard/monthly`):
- Aggregated monthly statistics
- Compare sentiment trends across months
- Identify seasonal patterns

### How to Use (End-to-End Workflow)

```
1. Start all services
   → docker-compose up -d
   
2. Preprocess data (one-time)
   → docker-compose --profile preprocessing up preprocessing-job
   
3. Run Spark streaming to generate predictions
   → docker-compose --profile streaming up streaming-job
   
4. Optionally trigger Kafka producer for scheduled batches
   → Enable in Airflow UI or run manually
   
5. View real-time analytics in dashboard
   → Open http://localhost:5173
   
6. Monitor MongoDB for data
   → Open MongoDB Express at http://localhost:9090
   
7. Check model drift weekly (Airflow)
   → DAG runs automatically or trigger manually
```

---

## API Documentation

### Base URL

```
http://localhost:8000
```

### Endpoints

#### Analytics

**GET `/api/stats`** — Overall sentiment statistics
```bash
curl http://localhost:8000/api/stats
```
Response:
```json
{
  "total": 1250,
  "positive": 750,
  "neutral": 300,
  "negative": 200
}
```

**GET `/api/predictions-by-date`** — Sentiment trends over time
```bash
curl http://localhost:8000/api/predictions-by-date
```
Response:
```json
{
  "labels": ["Jan'26", "Feb'26", "Mar'26"],
  "positive": [500, 520, 550],
  "neutral": [200, 220, 240],
  "negative": [100, 110, 130]
}
```

**GET `/api/top-products?limit=10`** — Top products by volume
```bash
curl "http://localhost:8000/api/top-products?limit=10"
```

**GET `/api/top-products-sentiment?limit=8`** — Top products with sentiment breakdown
```bash
curl "http://localhost:8000/api/top-products-sentiment?limit=8"
```

#### Predictions

**GET `/api/recent-predictions?limit=20`** — Latest predictions
```bash
curl "http://localhost:8000/api/recent-predictions?limit=20"
```

**GET `/api/product-scoring?product_id=B00IGGY4K2`** — Sentiment for specific product
```bash
curl "http://localhost:8000/api/product-scoring?product_id=B00IGGY4K2"
```

**GET `/api/product-scoring/{product_id}`** — Alternative syntax
```bash
curl "http://localhost:8000/api/product-scoring/B00IGGY4K2"
```

#### Streaming

**GET `/api/stream/stats`** — Server-Sent Events for real-time updates
```bash
curl -N http://localhost:8000/api/stream/stats
```
Receives updates every second with updated stats and predictions.

#### Model Insights

**GET `/api/drift-status`** — Latest model drift detection results
```bash
curl http://localhost:8000/api/drift-status
```

**GET `/api/model-insights`** — Model metrics from latest evaluation
```bash
curl http://localhost:8000/api/model-insights
```

**GET `/api/aggregation-status`** — Status of nightly aggregations
```bash
curl http://localhost:8000/api/aggregation-status
```

### Interactive API Docs

Swagger UI: **http://localhost:8000/docs**
ReDoc: **http://localhost:8000/redoc**

---

## Monitoring & Debugging

### Docker Compose Monitoring

```bash
# View all running containers
docker-compose ps

# Stream logs from all services
docker-compose logs -f

# Stream logs from specific service
docker-compose logs -f api
docker-compose logs -f spark-master
docker-compose logs -f streaming-job

# View last 100 lines
docker-compose logs --tail=100

# View logs with timestamps
docker-compose logs -t
```

### MongoDB Inspection

```bash
# Access MongoDB shell
docker exec -it mongodb mongosh -u admin -p admin123

# In MongoDB shell:
use amazon_reviews
db.predictions.countDocuments()          # Total predictions
db.predictions.find().limit(1)           # Sample prediction
db.agg_monthly_sentiments.find()         # Aggregated stats
db.drift_status.findOne()                # Latest drift alert
```

Or use **MongoDB Express UI**: http://localhost:9090

### Kafka Inspection

```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092

# Monitor messages in real-time
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic reviews.raw \
  --from-beginning
```

Or use **Kafka UI**: http://localhost:8090

### Spark Monitoring

**Spark Master UI**: http://localhost:8080
- View active jobs, stages, executors
- Monitor memory and CPU usage

**Spark Worker UIs**:
- Worker 1: http://localhost:8081
- Worker 2: http://localhost:8082

### Airflow Monitoring

**Airflow UI**: http://localhost:8888
- View DAG runs and task logs
- Trigger DAGs manually
- Monitor SLAs and retries

### API Testing

```bash
# Test backend health
curl http://localhost:8000/docs

# Test a specific endpoint
curl -s http://localhost:8000/api/stats | jq .

# Test frontend with verbose
curl -v http://localhost:5173
```

### Common Issues & Fixes

| Issue | Solution |
|-------|----------|
| "Kafka not reachable" | Ensure Zookeeper is running first: `docker-compose logs kafka` |
| "MongoDB connection refused" | Wait 10s for MongoDB to start; check: `docker exec mongodb mongosh --eval "db.version()"` |
| "Spark worker fails to connect" | Verify network: `docker network ls` and check `amazon_reviews_network` exists |
| "Frontend shows empty dashboard" | Check API logs: `docker-compose logs api` and MongoDB: `docker exec -it mongodb mongosh` |
| "Out of memory" | Increase Docker memory limit (Docker Desktop settings) |

---

## Development Notes

### Local Frontend Development

Without Docker, develop the frontend with hot-reload:

```bash
cd frontend

# Install dependencies
npm install

# Start dev server (auto-reload on code changes)
npm run dev

# Frontend will run at http://localhost:5173
# It will proxy API calls to http://localhost:8000
```

### Local API Development

Without Docker, develop the FastAPI backend:

```bash
cd api

# Create Python venv
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start server with auto-reload
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Notebook Development

Jupyter notebooks are pre-configured for ML experimentation:

```bash
# Install Jupyter (if not already installed)
pip install jupyter

# Start Jupyter server
jupyter notebook

# Open notebooks/ directory
```

Notebooks walk through:
1. EDA & data profiling
2. Feature engineering & preprocessing
3. Model selection & hyperparameter tuning
4. Training final Logistic Regression model
5. Evaluation & performance metrics

### Key Environment Variables

```env
# MongoDB
MONGO_URI                           # Full connection string (optional; uses user/host/port if not set)
MONGODB_ROOT_USER                   # MongoDB admin username
MONGODB_ROOT_PASSWORD               # MongoDB admin password
MONGODB_DATABASE                    # Database name
MONGODB_HOST                        # MongoDB container hostname
MONGODB_PORT                        # MongoDB port (27017)
MONGO_COLLECTION                    # Main predictions collection
MONGO_COLLECTION_MONTHLY            # Aggregated monthly stats collection
MONGO_COLLECTION_PRODUCT_SCORING    # Product-level aggregations

# Kafka
KAFKA_BOOTSTRAP                     # Bootstrap servers for Kafka
KAFKA_TOPIC                         # Topic name for raw reviews
KAFKA_TOPIC_PARTITIONS              # Number of partitions
KAFKA_TOPIC_REPLICATION             # Replication factor

# Spark
SPARK_MODE                          # master or worker
SPARK_MASTER_URL                    # URL for workers to connect to master

# API
FRONTEND_ORIGINS                    # CORS allowed origins
TARGET_PRODUCT                      # Default product for dashboards

# Airflow
AIRFLOW_HOME                        # Airflow working directory
```

### Deployment Considerations

For production:

1. **Security**:
   - Change default MongoDB credentials (`.env`)
   - Set strong Airflow admin passwords
   - Use production API keys for Kafka if required
   - Enable HTTPS for API and frontend

2. **Scalability**:
   - Add more Spark workers for horizontal scaling
   - Use managed MongoDB (e.g., MongoDB Atlas)
   - Implement Kafka topic partitioning strategy
   - Cache frequently-used aggregations in Redis

3. **Monitoring**:
   - Set up Prometheus + Grafana for metrics
   - Configure alerting for drift detection
   - Log to centralized ELK stack
   - Monitor data quality metrics

4. **Performance**:
   - Add database indexes on `ProductId`, `PredictedSentiment`
   - Implement pagination for large result sets
   - Use read replicas for MongoDB
   - Profile and optimize PySpark jobs

---

## License & Attribution

Dataset: [Amazon Fine Food Reviews](https://www.kaggle.com/datasets/snap/amazon-fine-food-reviews)

For more information, see [PERMISSION.md](PERMISSION.md).

---

**Last Updated**: May 7, 2026  
**Maintainer**: Data Engineering Team