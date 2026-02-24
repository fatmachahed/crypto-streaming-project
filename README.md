# 📈 Crypto Real-Time Streaming

Streaming pipeline temps réel de prix de cryptomonnaies.

## Architecture
```
CoinGecko API → Kafka → Spark Streaming → ML → Streamlit
```

## Stack
- **Kafka** : ingestion des données
- **Spark Structured Streaming** : traitement temps réel
- **Isolation Forest** : détection d'anomalies
- **Streamlit + Plotly** : visualisation temps réel

## Installation
```bash
pip install -r requirements.txt
```

## Lancement
```bash
# 1. Kafka
docker-compose up -d

# 2. Spark
python spark/streaming_app.py

# 3. Producer
python kafka/producer.py

# 4. Dashboard
streamlit run dashboard/app.py
```

## Équipe
- Personne 1 : Kafka + Infrastructure
- Personne 2 : Spark + ML
- Personne 3 : Dashboard + Documentation



┌─────────────────────────────────────────────────────┐
│  Personne 1 — Ingestion & Infrastructure            │
│                                                     │
│  ✅ docker-compose.yml (Kafka + Zookeeper)          │
│  ✅ kafka/producer.py  (API CoinGecko → Kafka)      │
│  ✅ requirements.txt                                │
│  ✅ .gitignore                                      │
│  ✅ Branche Git: feature/kafka-producer             │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  Personne 2 — Spark Streaming & ML                  │
│                                                     │
│  ✅ spark/streaming_app.py                          │
│     - lecture Kafka                                 │
│     - data cleaning                                 │
│     - Isolation Forest (anomaly detection)          │
│     - aggregations                                  │
│     - écriture output CSV                           │
│  ✅ Branche Git: feature/spark-ml                   │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  Personne 3 — Dashboard & Documentation             │
│                                                     │
│  ✅ dashboard/app.py (Streamlit + Plotly)           │
│     - KPIs temps réel                               │
│     - 4 graphiques interactifs                      │
│     - table anomalies                               │
│     - auto-refresh toutes les 3s                    │
│  ✅ README.md                                       │
│  ✅ Branche Git: feature/dashboard                  │
└─────────────────────────────────────────────────────┘