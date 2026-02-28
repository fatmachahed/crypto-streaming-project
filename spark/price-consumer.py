import json
import sqlite3
import logging

from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC     = "crypto-prices"
DB_PATH         = "../data/crypto_data.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RAW-CONSUMER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

"""
consome les prix bruts validés du topic Kafka 'crypto-prices'
et les écrit dans la table raw_prices de SQLite.
"""

def main():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        group_id="raw-price-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    log.info(f"Consuming raw prices from '{INPUT_TOPIC}' ...")

    for msg in consumer:
        try:
            d = msg.value
            conn.execute("""
                INSERT INTO raw_prices (symbol, price_usd, change_24h, volume_24h, market_cap, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                d.get("symbol"),
                d.get("price_usd"),
                d.get("change_24h"),
                d.get("volume_24h"),
                d.get("market_cap"),
                d.get("timestamp"),
            ))
            conn.commit()
            log.info(f"{d.get('symbol'):>4} | ${d.get('price_usd'):>12,.2f}")
        except Exception as e:
            log.error(f"Error: {e}")


if __name__ == "__main__":
    main()