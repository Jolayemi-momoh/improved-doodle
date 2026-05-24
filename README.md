# 🌍 Distributed Spatio-Temporal Crop Stress & Yield Pipeline

*A production-grade, event-driven machine learning pipeline built for the IAPrecision Applied AI & Remote Sensing Fellowship.*

## 🎯 The Mission
The future of African agriculture relies on precise, data-driven decisions. This project moves beyond standalone, theoretical models by implementing a highly scalable, distributed architecture. It dynamically ingests multispectral satellite/UAV telemetry, extracts spatial stress features, and predicts harvest yields to inform socio-economic forecasting and post-harvest infrastructure planning.

## 🏗️ Technical Architecture
This project utilizes a modern decoupled architecture, replacing legacy systems with event-driven data flows:

1. **Ingestion (Google Earth Engine API):** Programmatic querying of Sentinel-2 Harmonized multispectral arrays (Red/NIR bands) to compute localized Normalized Difference Vegetation Index (NDVI) payloads.
2. **Streaming Engine (Kafka KRaft):** A modern, Zookeeper-less Apache Kafka broker handles high-throughput telemetry streams from spatial coordinates, decoupling ingestion from storage.
3. **Storage (MongoDB):** A NoSQL document database acts as the durable sink for the Kafka consumer, storing spatial metadata.
4. **Computer Vision (PyTorch):** A Convolutional Neural Network (CNN) architecture designed to extract 128-dimensional spatial feature vectors from multispectral drone image patches.
5. **Predictive Modeling (XGBoost):** An ensemble gradient boosting regressor that fuses spatial telemetry with time-series environmental data to predict end-of-season yield (tons/hectare).

## 🚀 How to Run the Pipeline

### 1. Provision the Infrastructure
Ensure Docker is installed, then spin up the Kafka (KRaft) and MongoDB containers:
```bash
docker-compose up -d