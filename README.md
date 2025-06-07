# 🚀 Enhanced Data Pipeline – by Samratha Reddy

This is a **production-grade, personalized data pipeline project** built by **Samratha Reddy** for professional showcase and deployment. It has been customized to suit real-world scenarios in **batch and streaming data processing**, leveraging modern tools such as **Apache Spark, Kafka, Airflow, PostgreSQL, MinIO**, and more.

**GitHub Repository:** [github.com/samrathareddy/Enhanced-Data-Pipeline](https://github.com/samrathareddy/Enhanced-Data-Pipeline)

---

## 📌 Key Enhancements & Contributions

* Refactored batch and streaming DAGs with robust error handling and schedule optimization
* Integrated parameterized Spark batch/streaming jobs
* Improved modularity in Airflow and Spark directories for ease of testing and extension
* Enhanced S3/MinIO and MongoDB data source compatibility
* Added detailed README with a developer-friendly walkthrough

---

## 🛠️ Tools & Technologies

* **Processing:** Apache Spark (Batch + Streaming), PySpark
* **Ingestion:** Apache Kafka, Airflow, Kafka Producer Scripts
* **Storage:** PostgreSQL, MinIO (S3-compatible), MongoDB
* **Orchestration:** Apache Airflow
* **Monitoring:** Prometheus, Grafana, Great Expectations
* **ML & BI:** MLflow, Feast, Tableau-ready integration
* **DevOps:** Docker, Docker Compose, GitHub Actions, Kubernetes (optional)

---

## 👨‍💻 About Me – Samratha Reddy

A hands-on **Data Engineer** and **Graduate Student at Cleveland State University**, I specialize in building scalable, real-time data solutions using cloud-native services and open-source tools. This project demonstrates my ability to customize and deploy complex data stacks for real business use cases including anomaly detection, batch ETL, and stream processing.

🔗 Connect: [LinkedIn](https://www.linkedin.com/in/samrathareddy)

---

## ⚙️ Quick Start Guide

```bash
# Clone the repository
https://github.com/samrathareddy/Enhanced-Data-Pipeline.git
cd Enhanced-Data-Pipeline

# Start all services
docker-compose up --build
```

📌 After services are running:

* Airflow UI → [http://localhost:8080](http://localhost:8080)
* Kafka & Producer Scripts → in `kafka/`
* PostgreSQL & MinIO accessible via Docker
* Monitoring via Grafana and Prometheus

---

## 🧩 Use Cases Demonstrated

* Real-Time Data Quality Monitoring (Spark Streaming + Kafka)
* Batch Ingestion from MySQL → MinIO → Spark → PostgreSQL
* Data Governance with Apache Atlas integration (stub)
* ML Experiment Tracking with MLflow

---

## 📁 Project Structure (Simplified)

```
├── airflow/              # DAGs and Airflow Docker setup
├── spark/                # Batch and streaming Spark jobs
├── kafka/                # Kafka setup and producer script
├── monitoring/           # Prometheus and Grafana
├── ml/                   # MLflow and Feast integrations
├── storage/              # S3, MongoDB, Hadoop stubs
├── terraform/            # Infrastructure as code (optional)
├── kubernetes/           # K8s manifests (optional)
├── docker-compose.yaml   # Docker service orchestration
```

---

## 🔐 Final Notes

This project was fully customized and tested by **Samratha Reddy** to reflect personal development skills in building modern data platforms. It serves as a **portfolio-grade example** and can be extended for:

* Real-time analytics
* IoT ingestion
* Data Lake pipelines
* ML/AI workflows

---

**⭐ Star this project if you like it — and feel free to fork and extend it yourself!**
