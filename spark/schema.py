from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
)

CRYPTO_SCHEMA = StructType([
    StructField("coin_id",    StringType(), nullable=True),
    StructField("symbol",     StringType(), nullable=True),
    StructField("price_usd",  DoubleType(), nullable=True),
    StructField("market_cap", DoubleType(), nullable=True),
    StructField("volume_24h", DoubleType(), nullable=True),
    StructField("change_24h", DoubleType(), nullable=True),
    StructField("timestamp",  StringType(), nullable=True),
])

VALID_SYMBOLS = ["BTC", "ETH", "SOL", "ADA"]

def _coin_id_ok():
    return F.col("coin_id").isNotNull() & (F.trim(F.col("coin_id")) != "")

def _symbol_ok():
    return F.col("symbol").isin(VALID_SYMBOLS)

def _price_ok():
    return (
        F.col("price_usd").isNotNull()
        & (F.col("price_usd") > 0)
        & (F.col("price_usd") < 10_000_000)  
    )

def _volume_ok():
    return F.col("volume_24h").isNotNull() & (F.col("volume_24h") >= 0)

def _change_ok():
    return (
        F.col("change_24h").isNotNull()
        & (F.col("change_24h") > -100)   # can't drop more than 100%
        & (F.col("change_24h") <  500)   # guard against API glitches
    )

def _timestamp_ok():
    return (
        F.col("timestamp").isNotNull()
        & F.to_timestamp(F.col("timestamp")).isNotNull()
    )

def _is_valid():
    return (
        _coin_id_ok()
        & _symbol_ok()
        & _price_ok()
        & _volume_ok()
        & _change_ok()
        & _timestamp_ok()
    )


def add_parsed_timestamp(df: DataFrame) -> DataFrame:
    """trsanformer la colonne timestamp de string à timestamp, en ajoutant une nouvelle colonne event_ts"""
    return df.withColumn("event_ts", F.to_timestamp(F.col("timestamp")))


def split_valid_invalid(df: DataFrame):
    """
    Sépare le DataFrame en deux :
- valid_df : lignes qui passent toutes les validations
- invalid_df : lignes qui échouent au moins une validation, avec une colonne "invalid_reason" indiquant la première validation qui a échoué"
    """
    valid_df = df.filter(_is_valid())

    reason = (
        F.when(~_coin_id_ok(),  F.lit("bad_coin_id"))
         .when(~_symbol_ok(),   F.lit("bad_symbol"))
         .when(~_price_ok(),    F.lit("bad_price"))
         .when(~_volume_ok(),   F.lit("bad_volume"))
         .when(~_change_ok(),   F.lit("bad_change_24h"))
         .when(~_timestamp_ok(),F.lit("bad_timestamp"))
         .otherwise(F.lit("unknown"))
    )

    invalid_df = df.filter(~_is_valid()).withColumn("invalid_reason", reason)

    return valid_df, invalid_df