import sys, os, pandas as pd, numpy as np
sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, count
from pyspark.sql.functions import max as spark_max, min as spark_min
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import StructType, DoubleType, StringType, TimestampType
from sklearn.linear_model import LinearRegression
from collections import defaultdict

if os.name == 'nt':
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CKPT_DIR   = os.path.join(BASE_DIR, "..", "checkpoints")
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "output")
os.makedirs(CKPT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Buffer historique prix par coin (en memoire) ──────────────────────────────
price_history = defaultdict(list)
MIN_POINTS    = 5

# ── Schema ────────────────────────────────────────────────────────────────────
schema = StructType() \
    .add("coin",       StringType()) \
    .add("timestamp",  StringType()) \
    .add("price_usd",  DoubleType()) \
    .add("change_24h", DoubleType()) \
    .add("volume_24h", DoubleType())

# ── Spark session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CryptoStreaming") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── Read from Kafka ───────────────────────────────────────────────────────────
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .load()

df = raw.select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*")

# ── Helpers ───────────────────────────────────────────────────────────────────
def fmt_volume(v):
    if v >= 1_000_000_000:
        return f"${v/1_000_000_000:.2f}B"
    elif v >= 1_000_000:
        return f"${v/1_000_000:.2f}M"
    return f"${v:,.0f}"

def predict_next_price(prices):
    if len(prices) < MIN_POINTS:
        return None, None
    X = np.arange(len(prices)).reshape(-1, 1)
    y = np.array(prices)
    model = LinearRegression()
    model.fit(X, y)
    predicted = model.predict(np.array([[len(prices)]]))[0]
    trend = "HAUSSE" if model.coef_[0] > 0 else "BAISSE"
    return builtins_round(predicted, 2), trend

# ── foreachBatch ──────────────────────────────────────────────────────────────
import builtins
builtins_round = builtins.round

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # ════════════════════════════════════════════
    # 1. DATA CLEANING
    # ════════════════════════════════════════════
    df_clean = batch_df \
        .dropna() \
        .filter(col("price_usd") > 0) \
        .filter(col("volume_24h") > 0) \
        .filter(col("change_24h").between(-100, 100)) \
        .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
        .dropDuplicates(["coin", "timestamp"])

    total_raw   = batch_df.count()
    total_clean = df_clean.count()

    if total_clean == 0:
        print(f"Batch {batch_id} — vide apres nettoyage")
        return

    # ════════════════════════════════════════════
    # 2. AGREGATIONS PAR COIN
    # ════════════════════════════════════════════
    agg = df_clean.groupBy("coin").agg(
        spark_round(avg("price_usd"),  2).alias("avg_price"),
        spark_round(spark_max("price_usd"), 2).alias("max_price"),
        spark_round(spark_min("price_usd"), 2).alias("min_price"),
        spark_round(spark_max("price_usd") - spark_min("price_usd"), 2).alias("price_range"),
        spark_round(avg("change_24h"), 4).alias("avg_change_24h"),
        spark_round(avg("volume_24h"), 0).alias("avg_volume_24h"),
        count("*").alias("nb_events")
    ).orderBy("coin")

    # ════════════════════════════════════════════
    # 3. WINDOWING — fenetre glissante 1 min / pas 30s
    # ════════════════════════════════════════════
    windowed = df_clean.groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("coin")
    ).agg(
        spark_round(avg("price_usd"),  2).alias("avg_price_window"),
        spark_round(spark_max("price_usd"), 2).alias("max_price_window"),
        spark_round(spark_min("price_usd"), 2).alias("min_price_window"),
        count("*").alias("nb_events_window")
    ).orderBy("window", "coin")

    # ════════════════════════════════════════════
    # 4. ML — REGRESSION LINEAIRE
    # ════════════════════════════════════════════
    pdf = df_clean.select("coin", "price_usd").toPandas()
    for coin, group in pdf.groupby("coin"):
        price_history[coin].extend(group["price_usd"].tolist())
        price_history[coin] = price_history[coin][-50:]

    predictions = {}
    for coin, prices in price_history.items():
        predicted, trend = predict_next_price(prices)
        predictions[coin] = {"predicted": predicted, "trend": trend, "n_points": len(prices)}

    # ════════════════════════════════════════════
    # AFFICHAGE
    # ════════════════════════════════════════════
    rows_agg = agg.collect()
    rows_win = windowed.collect()
    dropped  = total_raw - total_clean

    print(f"\n{'='*75}")
    print(f"  BATCH {batch_id}  |  {total_raw} evt recus  ->  {total_clean} propres  |  {dropped} filtres")
    print(f"{'='*75}")

    print(f"\n  [CLEANING]")
    print(f"  Suppression nulls, prix<=0, volume<=0, variation hors [-100,100], doublons")
    print(f"  {dropped} evenement(s) filtre(s) sur {total_raw}")

    print(f"\n  [AGREGATIONS] par coin")
    print(f"  {'COIN':<15} {'AVG $':>12} {'MAX $':>12} {'MIN $':>12} {'RANGE':>10} {'CHG 24h':>9} {'VOLUME':>12}")
    print(f"  {'-'*85}")
    most_volatile = builtins.max(rows_agg, key=lambda r: r.price_range)
    for row in rows_agg:
        chg  = f"{row.avg_change_24h:+.2f}%"
        vol  = fmt_volume(row.avg_volume_24h)
        flag = " *" if row.coin == most_volatile.coin else ""
        print(f"  {row.coin:<15} {row.avg_price:>12,.2f} {row.max_price:>12,.2f} "
              f"{row.min_price:>12,.2f} {row.price_range:>10,.2f} {chg:>9} {vol:>12}{flag}")
    print(f"  (* coin le plus volatil du batch)")

    print(f"\n  [WINDOWING] fenetre 1 min / pas 30s")
    print(f"  {'WINDOW':>10} {'COIN':<15} {'AVG $':>12} {'MAX $':>12} {'MIN $':>12} {'N':>5}")
    print(f"  {'-'*68}")
    for row in rows_win:
        wstart = row.window.start.strftime("%H:%M:%S")
        print(f"  {wstart:>10} {row.coin:<15} {row.avg_price_window:>12,.2f} "
              f"{row.max_price_window:>12,.2f} {row.min_price_window:>12,.2f} {row.nb_events_window:>5}")

    print(f"\n  [ML] Regression lineaire — prediction prochain prix")
    print(f"  {'COIN':<15} {'PRIX ACTUEL':>14} {'PRIX PREDIT':>14} {'DIFF':>10} {'TENDANCE':>10} {'HISTORIQUE':>12}")
    print(f"  {'-'*80}")
    for row in rows_agg:
        coin = row.coin
        pred = predictions.get(coin, {})
        if pred.get("predicted"):
            diff     = pred["predicted"] - row.avg_price
            diff_str = f"{diff:+.2f}"
            print(f"  {coin:<15} {row.avg_price:>14,.2f} {pred['predicted']:>14,.2f} "
                  f"{diff_str:>10} {pred['trend']:>10} {pred['n_points']:>10} pts")
        else:
            pts = pred.get("n_points", 0)
            print(f"  {coin:<15} {row.avg_price:>14,.2f} {'...en attente':>14} "
                  f"{'':>10} {'':>10} {pts:>10} pts  (min {MIN_POINTS} requis)")

    # ════════════════════════════════════════════
    # SAUVEGARDE
    # ════════════════════════════════════════════
    agg_path  = os.path.join(OUTPUT_DIR, "crypto_agg.csv")
    pred_path = os.path.join(OUTPUT_DIR, "crypto_predictions.csv")

    agg.toPandas().to_csv(agg_path, mode="a", header=not os.path.exists(agg_path), index=False)

    pred_df = pd.DataFrame([
        {"coin": c, "predicted_price": v["predicted"], "trend": v["trend"], "batch_id": batch_id}
        for c, v in predictions.items() if v["predicted"] is not None
    ])
    if not pred_df.empty:
        pred_df.to_csv(pred_path, mode="a", header=not os.path.exists(pred_path), index=False)

# ── Start ─────────────────────────────────────────────────────────────────────
df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", CKPT_DIR) \
    .start() \
    .awaitTermination()