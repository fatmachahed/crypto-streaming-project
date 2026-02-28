import os, time 
import time
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="📈 Crypto Streaming", layout="wide")

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
CLEAN_PATH   = os.path.join(BASE_DIR, "..", "output", "crypto_clean.csv")
PRED_PATH    = os.path.join(BASE_DIR, "..", "output", "crypto_predictions.csv")
ANOMALY_PATH = os.path.join(BASE_DIR, "..", "output", "crypto_anomalies.csv")

# ── connect to SQLITE  ─────────────────────────────
DB_PATH = os.path.join(BASE_DIR, "..", "output", "crypto.db")

# --- Fonction pour vérifier si une table existe ---
def table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    exists = cursor.fetchone() is not None
    cursor.close()
    return exists

# --- Lecture des tables avec contrôle ---
conn = sqlite3.connect(DB_PATH)

# crypto_clean
if table_exists(conn, "crypto_clean"):
    df = pd.read_sql("SELECT * FROM crypto_clean ORDER BY timestamp DESC LIMIT 6000", conn)
elif os.path.exists(CLEAN_PATH):
    st.warning("Table crypto_clean non trouvée. Chargement depuis CSV...")
    df = pd.read_csv(CLEAN_PATH)
else:
    st.warning("Pas de données disponibles pour crypto_clean.")
    df = pd.DataFrame()

# crypto_predictions
if table_exists(conn, "crypto_predictions"):
    df_pred = pd.read_sql("SELECT * FROM crypto_predictions", conn)
elif os.path.exists(PRED_PATH):
    df_pred = pd.read_csv(PRED_PATH)
else:
    df_pred = pd.DataFrame()

# crypto_anomalies
if table_exists(conn, "crypto_anomalies"):
    df_anomaly = pd.read_sql("SELECT * FROM crypto_anomalies", conn)
elif os.path.exists(ANOMALY_PATH):
    df_anomaly = pd.read_csv(ANOMALY_PATH)
else:
    df_anomaly = pd.DataFrame()

conn.close()


st.title("📈 Real-Time Crypto Streaming Dashboard")
st.caption("Kafka + Spark Structured Streaming + Z-Score Anomaly Detection + Linear Regression")

if df.empty:
    st.warning("⏳ En attente de données... Lance le producer et Spark.")
    time.sleep(3)
    st.experimental_rerun()

# ── Load data ─────────────────────────────
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
if df["timestamp"].dt.tz is None:
    df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
df["timestamp"] = df["timestamp"].dt.tz_convert("Africa/Tunis").dt.tz_localize(None)
df = df.dropna(subset=["timestamp"])
latest = df.sort_values("timestamp").groupby("coin").last().reset_index()

# ── Last update ───────────────────────────
st.success(f"🔄 {pd.Timestamp.now().strftime('%H:%M:%S')} — {len(df)} events reçus")

# ── KPIs ──────────────────────────────────
cols = st.columns(len(latest))
for i, row in latest.iterrows():
    delta_color = "normal" if row["change_24h"] >= 0 else "inverse"
    cols[i].metric(
        label=f"🪙 {row['coin'].upper()}",
        value=f"${row['price_usd']:,.2f}",
        delta=f"{row['change_24h']:+.2f}%"
    )
    
    
st.divider()
st.subheader(" 🕯️Candlestick — Sélectionne un coin")

# Couleurs par coin
COIN_COLORS = {
    "bitcoin":     "#F7931A",
    "ethereum":    "#627EEA",
    "binancecoin": "#F3BA2F",
    "solana":      "#9945FF",
    "cardano":     "#0033AD",
}

# Boutons radio horizontaux 
coins_available = sorted(df["coin"].unique())
selected_coin = st.radio(
    label="",
    options=coins_available,
    horizontal=True, 
    format_func=lambda c: f"🟡 {c.upper()}" if c == "binancecoin" else
                          f"🟠 {c.upper()}" if c == "bitcoin" else
                          f"🔵 {c.upper()}" if c == "ethereum" else
                          f"🟣 {c.upper()}" if c == "solana" else
                          f"🔷 {c.upper()}"
)

# Candlestick avec couleur du coin sélectionné
coin_color = COIN_COLORS.get(selected_coin, "#00CC96")

df_coin = df[df["coin"] == selected_coin].copy()
df_coin = df_coin.set_index("timestamp").resample("1min").agg(
    open=("price_usd", "first"),
    high=("price_usd", "max"),
    low=("price_usd", "min"),
    close=("price_usd", "last")
).dropna()

if len(df_coin) > 1:
    fig_candle = go.Figure(data=[go.Candlestick(
        x=df_coin.index,
        open=df_coin["open"],
        high=df_coin["high"],
        low=df_coin["low"],
        close=df_coin["close"],
        increasing_line_color=coin_color,   # hausse
        decreasing_line_color="#EF5350",  # baisse
        increasing_fillcolor=coin_color,
        decreasing_fillcolor="#EF5350",
    )])
    fig_candle.update_layout(
        title=f"{selected_coin.upper()} — Candlestick 1min",
        xaxis_rangeslider_visible=False,
        height=400,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig_candle, use_container_width=True)
else:
    st.info(f"⏳ Pas encore assez de données pour {selected_coin}...")
    
    
st.divider()

# ── Row 1  ──
c1, c2 = st.columns([4, 3]) 

with c1:
    st.subheader("💰 Prix en temps réel")
    if len(df) > 1:
        df_plot = df.copy()
        df_plot["minute"] = df_plot["timestamp"].dt.floor("min")
        df_plot = df_plot.groupby(["coin", "minute"]).last().reset_index()

        if len(df_plot["minute"].unique()) > 1:
            fig = px.line(
                df_plot.sort_values("minute"),
                x="minute", y="price_usd",
                color="coin",
                facet_row="coin",
                markers=True,
                title="Évolution par minute",
                height=800  
            )
            fig.update_yaxes(matches=None, showticklabels=True)
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            fig = px.bar(
                latest, x="coin", y="price_usd",
                color="coin", text="price_usd",
                title="Prix actuels", height=800
            )
            fig.update_traces(texttemplate="$%{text:,.2f}")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("⏳ En attente de plus de données...")

with c2:
    # Graphe 1 
    st.subheader("📊 Variation 24h")
    fig2 = px.bar(
        latest.sort_values("change_24h"),
        x="coin", y="change_24h",
        color="change_24h",
        color_continuous_scale="RdYlGn",
        text="change_24h",
        height=390 
    )
    fig2.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig2.update_layout(height=390, margin=dict(t=30, b=10))
    st.plotly_chart(fig2, use_container_width=True)

    # Graphe 2 
    st.subheader("🫧 Volume relatif")
    fig_bubble = px.scatter(
        latest,
        x="coin", y="change_24h",
        size="volume_24h",
        color="change_24h",
        color_continuous_scale="RdYlGn",
        hover_data=["price_usd", "volume_24h"],
        text="coin",
        title="Taille = Volume 24h",
        height=390 
    )
    fig_bubble.update_traces(textposition="top center")
    fig_bubble.update_layout(height=390, showlegend=False, margin=dict(t=30, b=10))
    st.plotly_chart(fig_bubble, use_container_width=True)
    
# ── Row 2 ─────────────────────────────────
c3, c4 = st.columns([4, 3])

with c3:
    st.subheader("🚨 Z-Score Anomaly Over Time")
    if "anomaly_score" in df.columns:
        df_plot = df[df["anomaly_score"] != 0].sort_values("timestamp")
        if not df_plot.empty:
            fig3 = px.scatter(
                df_plot,
                x="timestamp", y="anomaly_score",
                color="coin",
                symbol="is_anomaly",
                title="Z-Score (X = anomalie détectée)"
            )
            fig3.update_layout(height=350)
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("⏳ En attente de 15 points min pour le Z-Score...")
    else:
        st.info("⏳ Anomaly score pas encore disponible...")

with c4:
    st.subheader("📦 Volume 24h")
    fig4 = px.bar(
        latest, x="coin", y="volume_24h",
        color="coin", log_y=True
    )
    fig4.update_layout(height=350, showlegend=False)
    st.plotly_chart(fig4, use_container_width=True)


# ── Predictions ───────────────────────────
st.divider()
st.subheader("🔮 Prédictions ML (Régression Linéaire)")
if not df_pred.empty: 
    df_pred_latest = df_pred.sort_values("batch_id").groupby("coin").last().reset_index()
    pred_cols = st.columns(len(df_pred_latest))
    for i, row in df_pred_latest.iterrows():
        current = latest[latest["coin"] == row["coin"]]["price_usd"].values
        current_price = current[0] if len(current) > 0 else 0
        diff = row["predicted_price"] - current_price
        trend_icon = "📈" if row["trend"] == "HAUSSE" else "📉"
        pred_cols[i].metric(
            label=f"{trend_icon} {row['coin'].upper()}",
            value=f"${row['predicted_price']:,.2f}",
            delta=f"{diff:+.2f} vs actuel"
        )
else:
    st.info("⏳ Prédictions disponibles après 15 batches...")

# ── Anomalies ─────────────────────────────
st.divider()
st.subheader("🚨 Alertes Anomalies")

if not df_anomaly.empty: 
    real_anomalies = df_anomaly[df_anomaly["status"] == "ANOMALIE"]
    if not real_anomalies.empty:
        st.error(f"⚠️ {len(real_anomalies)} anomalies détectées !")
        st.dataframe(real_anomalies.tail(20), use_container_width=True)
    else:
        st.success("Aucune anomalie détectée pour l'instant")
else:
    st.success("Aucune anomalie détectée pour l'instant")

# ── Raw data ──────────────────────────────
with st.expander("Données brutes (50 derniers events)"):
    st.dataframe(
        df.sort_values("timestamp", ascending=False).head(50),
        use_container_width=True
    )

# ── Auto refresh ──────────────────────────
time.sleep(10)
st.experimental_rerun()