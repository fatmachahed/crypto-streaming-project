
from pyspark.sql.functions import col, window, avg, count
from pyspark.sql.functions import max as spark_max, min as spark_min
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import TimestampType
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, avg, count, window
from pyspark.sql.functions import max as spark_max, min as spark_min, round as spark_round
from collections import defaultdict

from ml_helpers import *
from storage import save_to_db
import os 

if os.name == 'nt':
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── foreachBatch ──────────────────────────────────────────────────────────────
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
        .withColumn("timestamp", col("timestamp")) \
        .dropDuplicates(["coin", "timestamp"])

    total_raw   = batch_df.count()
    total_clean = df_clean.count()

    if total_clean == 0:
        print(f"Batch {batch_id} -- vide apres nettoyage")
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

    rows_agg = agg.collect()

    if not rows_agg:
        print(f"Batch {batch_id} -- aggregation vide, skip")
        return

    # ════════════════════════════════════════════
    # 3. WINDOWING -- fenetre glissante 1 min / pas 30s
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

    rows_win = windowed.collect()

    # ════════════════════════════════════════════
    # 4. ML -- Mise a jour du buffer
    # ════════════════════════════════════════════
    pdf = df_clean.select("coin", "price_usd").toPandas()

    for coin, group in pdf.groupby("coin"):
        # FIX : arrondi a PRICE_DECIMALS pour eviter faux positifs Z-Score
        rounded_prices = [builtins.round(p, PRICE_DECIMALS) for p in group["price_usd"].tolist()]
        price_history[coin].extend(rounded_prices)
        price_history[coin] = price_history[coin][-50:]

    # ── 4a. REGRESSION LINEAIRE ───────────────
    predictions = {}
    for coin, prices in price_history.items():
        predicted, trend = predict_next_price(prices)
        predictions[coin] = {"predicted": predicted, "trend": trend, "n_points": len(prices)}

    # ── 4b. ANOMALY DETECTION Z-SCORE ─────────
    anomaly_results = {}
    for row in rows_agg:
        coin          = row.coin
        current_price = builtins.round(float(row.avg_price), PRICE_DECIMALS)
        prices        = price_history.get(coin, [])
        status, z     = detect_anomaly_zscore(prices, current_price)

        if status is None:
            anomaly_results[coin] = {
                "status"  : "en attente",
                "z_score" : None,
                "mean"    : None,
                "std"     : None,
                "n_points": len(prices)
            }
        else:
            prices_arr = np.array(prices)
            anomaly_results[coin] = {
                "status"  : status,
                "z_score" : z,
                "mean"    : builtins.round(float(np.mean(prices_arr)), 4),
                "std"     : builtins.round(float(np.std(prices_arr)),  4),
                "n_points": len(prices)
            }

    # ════════════════════════════════════════════
    # AFFICHAGE
    # ════════════════════════════════════════════
    dropped = total_raw - total_clean

    print(f"\n{'='*75}")
    print(f"  BATCH {batch_id}  |  {total_raw} evt recus  ->  {total_clean} propres  |  {dropped} filtres")
    print(f"{'='*75}")

    # -- Cleaning
    print(f"\n  [CLEANING]")
    print(f"  Suppression nulls, prix<=0, volume<=0, variation hors [-100,100], doublons")
    print(f"  {dropped} evenement(s) filtre(s) sur {total_raw}")

    # -- Agregations
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

    # -- Windowing
    print(f"\n  [WINDOWING] fenetre 1 min / pas 30s")
    print(f"  {'WINDOW':>10} {'COIN':<15} {'AVG $':>12} {'MAX $':>12} {'MIN $':>12} {'N':>5}")
    print(f"  {'-'*68}")
    for row in rows_win:
        wstart = row.window.start.strftime("%H:%M:%S")
        print(f"  {wstart:>10} {row.coin:<15} {row.avg_price_window:>12,.2f} "
              f"{row.max_price_window:>12,.2f} {row.min_price_window:>12,.2f} {row.nb_events_window:>5}")

    # -- Regression
    print(f"\n  [ML - REGRESSION] prediction prochain prix")
    print(f"  {'COIN':<15} {'PRIX ACTUEL':>14} {'PRIX PREDIT':>14} {'DIFF':>10} {'TENDANCE':>10} {'HIST':>8}")
    print(f"  {'-'*78}")
    for row in rows_agg:
        coin = row.coin
        pred = predictions.get(coin, {})
        if pred.get("predicted") is not None:
            diff     = pred["predicted"] - row.avg_price
            diff_str = f"{diff:+.4f}"
            print(f"  {coin:<15} {row.avg_price:>14,.4f} {pred['predicted']:>14,.4f} "
                  f"{diff_str:>10} {pred['trend']:>10} {pred['n_points']:>6} pts")
        else:
            pts = pred.get("n_points", 0)
            print(f"  {coin:<15} {row.avg_price:>14,.4f} {'...en attente':>14} "
                  f"{'':>10} {'':>10} {pts:>6} pts  (min {MIN_POINTS})")

    # -- Anomalie Z-Score
    print(f"\n  [ML - ANOMALIE] Z-Score sur prix (seuil |z| > {ZSCORE_THRESHOLD})")
    print(f"  {'COIN':<15} {'STATUT':>10} {'Z-SCORE':>10} {'MOYENNE':>12} {'STD':>10} {'HIST':>8}")
    print(f"  {'-'*70}")
    for row in rows_agg:
        coin   = row.coin
        result = anomaly_results.get(coin, {"status": "en attente", "z_score": None, "mean": None, "std": None, "n_points": 0})
        if result["z_score"] is not None:
            z_str    = f"{result['z_score']:+.4f}"
            mean_str = f"{result['mean']:,.4f}"
            std_str  = f"{result['std']:.4f}"
            flag     = "  <--- !!!" if result["status"] == "ANOMALIE" else ""
            print(f"  {coin:<15} {result['status']:>10} {z_str:>10} {mean_str:>12} {std_str:>10} {result['n_points']:>6} pts{flag}")
        else:
            print(f"  {coin:<15} {'en attente':>10} {'N/A':>10} {'N/A':>12} {'N/A':>10} {result['n_points']:>6} pts")

    # ════════════════════════════════════════════
    # SAUVEGARDE
    # ════════════════════════════════════════════
    agg_path     = os.path.join(OUTPUT_DIR, "crypto_agg.csv")
    pred_path    = os.path.join(OUTPUT_DIR, "crypto_predictions.csv")
    anomaly_path = os.path.join(OUTPUT_DIR, "crypto_anomalies.csv")
    clean_path = os.path.join(OUTPUT_DIR, "crypto_clean.csv")
    
    
    pdf_clean = df_clean.toPandas()
    pdf_clean["timestamp"] = pdf_clean["timestamp"].astype(str)  
    pdf_clean["anomaly_score"] = pdf_clean["coin"].map(
        lambda c: anomaly_results.get(c, {}).get("z_score", 0.0) or 0.0
    )
    pdf_clean["is_anomaly"] = pdf_clean["coin"].map(
        lambda c: 1 if anomaly_results.get(c, {}).get("status") == "ANOMALIE" else 0
    )
    pdf_clean.to_csv(clean_path, mode="a", header=not os.path.exists(clean_path), index=False)
    agg.toPandas().to_csv(agg_path, mode="a", header=not os.path.exists(agg_path), index=False)

    pred_df = pd.DataFrame([
        {"coin": c, "predicted_price": v["predicted"], "trend": v["trend"], "batch_id": batch_id}
        for c, v in predictions.items() if v["predicted"] is not None
    ])
    if not pred_df.empty:
        pred_df.to_csv(pred_path, mode="a", header=not os.path.exists(pred_path), index=False)

    anomaly_df = pd.DataFrame([
        {
            "coin"      : c,
            "status"    : v["status"],
            "z_score"   : v["z_score"],
            "mean_price": v["mean"],
            "std_price" : v["std"],
            "batch_id"  : batch_id
        }
        for c, v in anomaly_results.items() if v["status"] != "en attente"
    ])
    if not anomaly_df.empty:
        anomaly_df.to_csv(anomaly_path, mode="a", header=not os.path.exists(anomaly_path), index=False)
        
    
    save_to_db(pdf_clean, agg, pred_df, anomaly_df)