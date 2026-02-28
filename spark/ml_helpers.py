#ml_helpers.py
import numpy as np
import builtins
from collections import defaultdict
from sklearn.linear_model import LinearRegression

# ── Buffers en memoire ────────────────────────────────────────────────────────
price_history    = defaultdict(list)
MIN_POINTS       = 15
ZSCORE_THRESHOLD = 3.0
PRICE_DECIMALS   = 4    # arrondir les prix 

# ── Helpers ───────────────────────────────────────────────────────────────────
def fmt_volume(v):
    if v >= 1_000_000_000:
        return f"${v/1_000_000_000:.2f}B"
    elif v >= 1_000_000:
        return f"${v/1_000_000:.2f}M"
    return f"${v:,.0f}"

def predict_next_price(prices):
    """Regression lineaire sur l'historique des prix."""
    if len(prices) < MIN_POINTS:
        return None, None
    X = np.arange(len(prices)).reshape(-1, 1)
    y = np.array(prices)
    model = LinearRegression()
    model.fit(X, y)
    predicted = model.predict(np.array([[len(prices)]]))[0]
    trend = "HAUSSE" if model.coef_[0] > 0 else "BAISSE"
    return builtins.round(predicted, 4), trend

def detect_anomaly_zscore(prices, current_price):
    """
    Z-Score sur l'historique des prix arrondis a PRICE_DECIMALS.
    ANOMALIE si |z| > ZSCORE_THRESHOLD (3.0).
    Retourne (status, z_score) ou (None, None) si pas assez de points.
    """
    if len(prices) < MIN_POINTS:
        return None, None

    prices_arr = np.array(prices)
    mean = np.mean(prices_arr)
    std  = np.std(prices_arr)

    # Si std trop petite (prix completement stable), pas d'anomalie
    if std < 1e-6:
        return "normal", 0.0

    z = (current_price - mean) / std
    status = "ANOMALIE" if builtins.abs(z) > ZSCORE_THRESHOLD else "normal"
    return status, builtins.round(float(z), 4)