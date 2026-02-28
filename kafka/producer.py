import json
import time
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError


TOPIC           = "crypto-prices"
POLL_INTERVAL   = 10 

COINS = {
    "bitcoin":   "BTC",
    "ethereum":  "ETH",
    "solana":    "SOL",
    "cardano":   "ADA",
}

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
PARAMS = {
    "ids":                    ",".join(COINS.keys()),
    "vs_currencies":          "usd",
    "include_market_cap":     "true",
    "include_24hr_vol":       "true",
    "include_24hr_change":    "true",
}

"""
Message format:
{
    "coin_id":    "bitcoin",
    "symbol":     "BTC",
    "price_usd":  67432.15,
    "market_cap": 1327000000000,
    "volume_24h": 28500000000,
    "change_24h": 2.34,
    "timestamp":  "2024-01-15T10:30:00"
}
"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
)


def fetch_prices() -> dict | None:
    try:
        resp = requests.get(COINGECKO_URL, params=PARAMS, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        log.error(f"API fetch failed: {e}")
        return None


def parse_and_send(raw: dict) -> int:
    sent = 0
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    for coin_id, symbol in COINS.items():
        coin_data = raw.get(coin_id)
        if not coin_data:
            log.warning(f"No data returned for {coin_id}, skipping.")
            continue

        message = {
            "coin_id":    coin_id,
            "symbol":     symbol,
            "price_usd":  coin_data.get("usd"),
            "market_cap": coin_data.get("usd_market_cap"),
            "volume_24h": coin_data.get("usd_24h_vol"),
            "change_24h": coin_data.get("usd_24h_change"),
            "timestamp":  ts,
        }

        if message["price_usd"] is None:
            log.warning(f"Missing price for {symbol}, skipping.")
            continue

        producer.send(TOPIC, value=message)
        sent += 1
        log.info(f"Sent | {symbol:>4} | ${message['price_usd']:>12,.2f} | change={message['change_24h']:>+.2f}%")

    producer.flush()
    return sent


def main():
    log.info(f"Starting producer → topic='{TOPIC}' | poll every {POLL_INTERVAL}s")
    log.info(f"Tracking: {', '.join(COINS.values())}")

    total_sent = 0
    iteration  = 0

    while True:
        iteration += 1
        log.info(f"--- Poll #{iteration} ---")

        raw = fetch_prices()
        if raw:
            sent = parse_and_send(raw)
            total_sent += sent
            log.info(f"Sent {sent} messages this poll | total={total_sent}")
        else:
            log.warning("Skipping this poll due to fetch error.")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Producer stopped.")
        producer.close()