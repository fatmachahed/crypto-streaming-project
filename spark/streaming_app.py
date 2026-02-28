import sys, os
sys.stdout.reconfigure(encoding='utf-8')
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, DoubleType, StringType
from ml_helpers import *
from processing import process_batch

if os.name == 'nt':
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CKPT_DIR   = os.path.join(BASE_DIR, "..", "checkpoints")
os.makedirs(CKPT_DIR, exist_ok=True)

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
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

# ── Read from Kafka ───────────────────────────────────────────────────────────
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:39092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .load()

df = raw.select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*")

# ── Start ─────────────────────────────────────────────────────────────────────
df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", CKPT_DIR) \
    .start() \
    .awaitTermination()