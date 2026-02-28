import os
import json
import logging
from collections import defaultdict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

from schema import CRYPTO_SCHEMA, add_parsed_timestamp, split_valid_invalid

logging.basicConfig(level=logging.INFO)

KAFKA_BOOTSTRAP       = "localhost:9092"
INPUT_TOPIC           = "crypto-prices"
AGG_TOPIC             = "crypto-aggregations"
PRED_TOPIC            = "crypto-predictions"
INVALID_TOPIC         = "crypto-invalid"

CHECKPOINT_BASE       = "/tmp/crypto_checkpoints"
WATERMARK_DELAY       = "30 seconds"
WINDOW_DURATION       = "2 minutes"
WINDOW_SLIDE          = "30 seconds"

price_history = defaultdict(list)
MAX_HISTORY   = 50 


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CryptoStreamingPipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE)
        .getOrCreate()
    )

def read_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS raw_json")
    )

def parse_json(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("data", F.from_json("raw_json", CRYPTO_SCHEMA))
        .select(
            "raw_json",
            F.col("data.coin_id").alias("coin_id"),
            F.col("data.symbol").alias("symbol"),
            F.col("data.price_usd").alias("price_usd"),
            F.col("data.market_cap").alias("market_cap"),
            F.col("data.volume_24h").alias("volume_24h"),
            F.col("data.change_24h").alias("change_24h"),
            F.col("data.timestamp").alias("timestamp"),
        )
    )

def windowed_aggregations(df: DataFrame) -> DataFrame:
    """
    Sliding window aggregation per coin:
      - avg_price, min_price, max_price
      - price_volatility (stddev)
      - avg_change_24h
      - total_volume
      - event_count
    """
    return (
        df.withWatermark("event_ts", WATERMARK_DELAY)
        .groupBy(
            F.window("event_ts", WINDOW_DURATION, WINDOW_SLIDE),
            "symbol",
        )
        .agg(
            F.round(F.avg("price_usd"),  2).alias("avg_price"),
            F.round(F.min("price_usd"),  2).alias("min_price"),
            F.round(F.max("price_usd"),  2).alias("max_price"),
            F.round(F.stddev("price_usd"), 4).alias("price_volatility"),
            F.round(F.avg("change_24h"), 4).alias("avg_change_24h"),
            F.round(F.sum("volume_24h"), 2).alias("total_volume"),
            F.count("*").alias("event_count"),
        )
        .select(
            F.col("window.start").cast("string").alias("window_start"),
            F.col("window.end").cast("string").alias("window_end"),
            "symbol",
            "avg_price",
            "min_price",
            "max_price",
            "price_volatility",
            "avg_change_24h",
            "total_volume",
            "event_count",
        )
    )

# Linear Regression 

def predict_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    spark = batch_df.sparkSession
    rows  = batch_df.select("symbol", "price_usd", "timestamp").collect()

    predictions = []

    batch_by_symbol = defaultdict(list)
    for row in rows:
        if row["price_usd"] is not None:
            batch_by_symbol[row["symbol"]].append(row["price_usd"])

    for symbol, new_prices in batch_by_symbol.items():
        # Update history
        price_history[symbol].extend(new_prices)
        if len(price_history[symbol]) > MAX_HISTORY:
            price_history[symbol] = price_history[symbol][-MAX_HISTORY:]

        history = price_history[symbol]
        n = len(history)

        if n < 3:
            continue

        data = [(float(i), float(p)) for i, p in enumerate(history)]
        schema = StructType([
            StructField("idx",   DoubleType(), False),
            StructField("price", DoubleType(), False),
        ])
        history_df = spark.createDataFrame(data, schema)

        assembler   = VectorAssembler(inputCols=["idx"], outputCol="features")
        features_df = assembler.transform(history_df)

        try:
            lr    = LinearRegression(featuresCol="features", labelCol="price", maxIter=20)
            model = lr.fit(features_df)

            next_idx_df = spark.createDataFrame([(float(n),)], ["idx"])
            next_feat   = assembler.transform(next_idx_df)
            pred_row    = model.transform(next_feat).collect()[0]
            predicted   = round(pred_row["prediction"], 2)

            current     = round(history[-1], 2)
            delta_pct   = round((predicted - current) / current * 100, 4) if current else 0.0
            r2          = round(float(model.summary.r2), 4)

            predictions.append({
                "symbol":           symbol,
                "current_price":    current,
                "predicted_price":  predicted,
                "delta_pct":        delta_pct,
                "r2_score":         r2,
                "history_size":     n,
                "batch_id":         batch_id,
                "timestamp":        rows[-1]["timestamp"],
            })

            print(
                f"[ML] {symbol:>4} | current=${current:>12,.2f} "
                f"| predicted=${predicted:>12,.2f} "
                f"| Δ={delta_pct:>+.2f}% | R²={r2:.4f} | n={n}"
            )

        except Exception as e:
            print(f"[ML] Regression failed for {symbol}: {e}")

    # ecrire predictions dans Kafka
    if predictions:
        pred_rows = [(json.dumps(p),) for p in predictions]
        pred_schema = StructType([StructField("value", StringType(), False)])
        pred_df = spark.createDataFrame(pred_rows, pred_schema)

        (
            pred_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", PRED_TOPIC)
            .save()
        )


def to_kafka_json(df: DataFrame, cols) -> DataFrame:
    return df.select(F.to_json(F.struct(*cols)).alias("value"))


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # etape 1: lire les données brutes de Kafka
    raw_df = read_kafka(spark)

    # etape 2: parser le JSON et ajouter une colonne event_ts de type timestamp
    parsed_df = parse_json(raw_df)
    parsed_df = add_parsed_timestamp(parsed_df)

    # etape 3: séparer les données valides des invalides
    valid_df, invalid_df = split_valid_invalid(parsed_df)

    valid_df.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 20) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/valid_console") \
        .start()

    # Aggregations → Kafka 
    agg_df = windowed_aggregations(valid_df)

    agg_cols = [
        "window_start", "window_end", "symbol",
        "avg_price", "min_price", "max_price",
        "price_volatility", "avg_change_24h", "total_volume", "event_count",
    ]

    to_kafka_json(agg_df, agg_cols) \
        .writeStream \
        .outputMode("update") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", AGG_TOPIC) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/agg_kafka") \
        .start()

    # Aggregations → console 
    agg_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/agg_console") \
        .start()

    # predictions → Kafka)
    valid_df.writeStream \
        .foreachBatch(predict_batch) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/ml_predictions") \
        .start()

    # Invalid records → Kafka 
    invalid_cols = [
        "coin_id", "symbol", "price_usd", "market_cap",
        "volume_24h", "change_24h", "timestamp", "invalid_reason",
    ]

    to_kafka_json(invalid_df, invalid_cols) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", INVALID_TOPIC) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/invalid_kafka") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()