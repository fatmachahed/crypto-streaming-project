import json, time, requests
from confluent_kafka import Producer
from datetime import datetime

COINS = ["bitcoin", "ethereum", "binancecoin", "solana", "cardano"]
producer = Producer({"bootstrap.servers": "localhost:39092"})  # ✅ port 39092

print("🚀 Producer started...")
while True:
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": ",".join(COINS),
                "vs_currencies": "usd",
                "include_24hr_change": "true",
                "include_24hr_vol": "true"
            },
            timeout=5
        )
        data = r.json()

        # ✅ Vérifier que la réponse est un dict valide
        if not isinstance(data, dict):
            print(f"⚠️ Rate limit ou erreur API, attente 30s...")
            time.sleep(30)
            continue

        for coin, v in data.items():
            event = {
                "coin":       coin,
                "timestamp":  datetime.utcnow().isoformat(),
                "price_usd":  float(v.get("usd", 0)),
                "change_24h": float(v.get("usd_24h_change", 0)),
                "volume_24h": float(v.get("usd_24h_vol", 0))
            }
            producer.produce("crypto_prices", json.dumps(event).encode())
            print(f"📡 {coin}: ${event['price_usd']:,.2f}")
        producer.flush()

    except Exception as e:
        print(f"Error: {e}")

    time.sleep(15)  # ✅ 15s au lieu de 2s pour éviter le rate limit