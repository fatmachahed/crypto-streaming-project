import os, pandas as pd, numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, DoubleType, StringType

if os.name == 'nt':
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(BASE_DIR, "..", "model", "anomaly_model.pkl")
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "output")
CKPT_DIR   = os.path.join(BASE_DIR, "..", "checkpoints")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "..", "model"), exist_ok=True)

# ── Train model once ──────────────────────
def train_model():
    from sklearn.ensemble import IsolationForest
    import joblib
    np.random.seed(42)
    data = pd.DataFrame({
        "price_usd":  np.random.lognormal(10, 0.5, 5000),
        "change_24h": np.random.normal(0, 3, 5000),
        "volume_24h": np.random.lognormal(20, 1, 5000)
    })
    model = IsolationForest(contamination=0.05, random_state=42)
    model.fit(data)
    import joblib
    joblib.dump(model, MODEL_PATH)
    print("✅ Model trained and saved")

if not os.path.exists(MODEL_PATH):
    train_model()

# ── Schema ────────────────────────────────
schema = StructType() \
    .add("coin", StringType()) \
    .add("timestamp", StringType()) \
    .add("price_usd", DoubleType()) \
    .add("change_24h", DoubleType()) \
    .add("volume_24h", DoubleType())

# ── Spark session ─────────────────────────
spark = SparkSession.builder \
    .appName("CryptoStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── Read from Kafka ───────────────────────
df_parsed = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("d")) \
    .select("d.*")

# ── Process each batch ────────────────────
def process_batch(batch_df, batch_id):
    import joblib
    if batch_df.isEmpty():
        return

    model = joblib.load(MODEL_PATH)
    pdf   = batch_df.toPandas()

    # 1. Cleaning
    pdf = pdf.dropna()
    pdf = pdf[pdf["price_usd"] > 0]
    pdf["timestamp"] = pd.to_datetime(pdf["timestamp"])

    # 2. ML - Anomaly Detection
    features = ["price_usd", "change_24h", "volume_24h"]
    pdf["anomaly_score"] = model.decision_function(pdf[features])
    pdf["is_anomaly"]    = (model.predict(pdf[features]) == -1).astype(int)

    # 3. Aggregation per coin
    agg = pdf.groupby("coin").agg(
        latest_price  = ("price_usd",  "last"),
        avg_change    = ("change_24h", "mean"),
        anomaly_count = ("is_anomaly", "sum"),
        event_count   = ("coin",       "count")
    ).reset_index()

    # 4. Save
    clean = os.path.join(OUTPUT_DIR, "crypto_clean.csv")
    agg_f = os.path.join(OUTPUT_DIR, "crypto_agg.csv")
    pdf.to_csv(clean, mode="a", header=not os.path.exists(clean), index=False)
    agg.to_csv(agg_f, mode="a", header=not os.path.exists(agg_f), index=False)

    print(f"\n=== Batch {batch_id} — {len(pdf)} events ===")
    print(pdf[["coin","price_usd","change_24h","is_anomaly"]].to_string())

# ── Start streaming ───────────────────────
df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", CKPT_DIR) \
    .start() \
    .awaitTermination()