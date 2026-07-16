The pipeline simulates a live transaction feed, cleans and enriches it in-flight, flags suspicious activity using custom fraud rules, stores it in a warehouse, transforms it into analytics-ready tables, and visualizes it on a dashboard — with orchestration handled by Airflow.

## Tech stack

| Layer | Tool |
|---|---|
| Streaming | Apache Kafka |
| Stream processing | Apache Spark (Structured Streaming) |
| Data warehouse | PostgreSQL |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| Dashboard | Metabase |
| Containerization | Docker / Docker Compose |
| Language | Python |

## Dataset

[Bank Transaction Dataset for Fraud Detection](https://www.kaggle.com/datasets/valakhorasani/bank-transaction-dataset-for-fraud-detection) (Kaggle) — 2,512 simulated bank transactions with fields including transaction amount, channel, customer demographics, login attempts, and account balance.

The dataset has no pre-labeled fraud column by design — fraud detection logic is built from scratch as part of this project.

## Fraud detection rules

Custom rules applied during stream processing, combined into a fraud score:

- **Excessive login attempts** — 4 or more attempts before a successful transaction
- **Large amount vs. balance** — transaction exceeds 50% of account balance
- **Rapid large transaction** — high-value transfer completed in under 10 seconds
- **Dormant account reactivation** — large transaction after 30+ days of inactivity

## Project status

This project is being built incrementally and documented as it progresses.

- [x] Environment setup (WSL2, Docker)
- [x] Dataset sourced and explored
- [x] Kafka running in Docker
- [x] Python producer streaming transactions to Kafka
- [ ] Spark structured streaming consumer + fraud logic
- [ ] PostgreSQL warehouse
- [ ] dbt models (staging → marts)
- [ ] Airflow orchestration
- [ ] Metabase dashboard

## Running it locally

Requirements: Docker Desktop, Python 3, WSL2 (if on Windows)

```bash
git clone https://github.com/GivenMoselakgomo/bank-fraud-pipeline.git
cd bank-fraud-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Start Kafka
docker compose up -d

# Start streaming transactions
python3 producer.py
```

(Full setup instructions for the complete pipeline will be added as each component is built.)

## Author

Given Moselakgomo
EOF

git add README.md
git commit -m "Add project README"
git push
