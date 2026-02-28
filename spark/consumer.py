import json
import sqlite3
import logging
import threading
from datetime import datetime

from kafka import KafkaConsumer

AGG_TOPIC       = "crypto-aggregations"
PRED_TOPIC      = "crypto-predictions"
DB_PATH         = "../data/crypto_data.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS aggregations (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            window_start  TEXT,
            window_end    TEXT,
            symbol        TEXT,
            avg_price     REAL,
            min_price     REAL,
            max_price     REAL,
            price_volatility REAL,
            avg_change_24h   REAL,
            total_volume  REAL,
            event_count   INTEGER,
            ingested_at   TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol          TEXT,
            current_price   REAL,
            predicted_price REAL,
            delta_pct       REAL,
            r2_score        REAL,
            history_size    INTEGER,
            batch_id        INTEGER,
            timestamp       TEXT,
            ingested_at     TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_prices (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT,
            price_usd   REAL,
            change_24h  REAL,
            volume_24h  REAL,
            market_cap  REAL,
            timestamp   TEXT,
            ingested_at TEXT DEFAULT (datetime('now'))
        )
    """)

    conn.commit()
    log.info(f"Database ready at {db_path}")
    return conn


def insert_aggregation(conn: sqlite3.Connection, data: dict):
    conn.execute("""
        INSERT INTO aggregations
          (window_start, window_end, symbol, avg_price, min_price, max_price,
           price_volatility, avg_change_24h, total_volume, event_count)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        data.get("window_start"),
        data.get("window_end"),
        data.get("symbol"),
        data.get("avg_price"),
        data.get("min_price"),
        data.get("max_price"),
        data.get("price_volatility"),
        data.get("avg_change_24h"),
        data.get("total_volume"),
        data.get("event_count"),
    ))
    conn.commit()


def insert_prediction(conn: sqlite3.Connection, data: dict):
    conn.execute("""
        INSERT INTO predictions
          (symbol, current_price, predicted_price, delta_pct,
           r2_score, history_size, batch_id, timestamp)
        VALUES (?,?,?,?,?,?,?,?)
    """, (
        data.get("symbol"),
        data.get("current_price"),
        data.get("predicted_price"),
        data.get("delta_pct"),
        data.get("r2_score"),
        data.get("history_size"),
        data.get("batch_id"),
        data.get("timestamp"),
    ))
    conn.commit()


def consume_aggregations(conn: sqlite3.Connection):
    consumer = KafkaConsumer(
        AGG_TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="agg-consumer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    log.info(f"Consuming aggregations from '{AGG_TOPIC}' ...")
    for msg in consumer:
        try:
            data = msg.value
            insert_aggregation(conn, data)
            log.info(
                f"AGG  | {data.get('symbol'):>4} "
                f"| avg=${data.get('avg_price'):>12,.2f} "
                f"| vol={data.get('price_volatility')} "
                f"| n={data.get('event_count')}"
            )
        except Exception as e:
            log.error(f"Failed to process aggregation message: {e} | raw={msg.value}")


def consume_predictions(conn: sqlite3.Connection):
    consumer = KafkaConsumer(
        PRED_TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="pred-consumer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    log.info(f"Consuming predictions from '{PRED_TOPIC}' ...")
    for msg in consumer:
        try:
            data = msg.value
            insert_prediction(conn, data)
            log.info(
                f"PRED | {data.get('symbol'):>4} "
                f"| current=${data.get('current_price'):>12,.2f} "
                f"| predicted=${data.get('predicted_price'):>12,.2f} "
                f"| Δ={data.get('delta_pct'):>+.2f}% "
                f"| R²={data.get('r2_score'):.4f}"
            )
        except Exception as e:
            log.error(f"Failed to process prediction message: {e} | raw={msg.value}")


def main():
    conn = init_db(DB_PATH)

    t1 = threading.Thread(target=consume_aggregations, args=(conn,), daemon=True)
    t2 = threading.Thread(target=consume_predictions,  args=(conn,), daemon=True)

    t1.start()
    t2.start()

    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        log.info("Consumer stopped.")


if __name__ == "__main__":
    main()