import streamlit as st
import pandas as pd
import plotly.express as px
import os, time

st.set_page_config(page_title="📈 Crypto Streaming", layout="wide")

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CLEAN_PATH = os.path.join(BASE_DIR, "..", "output", "crypto_clean.csv")

st.title("📈 Real-Time Crypto Streaming Dashboard")
st.caption("Kafka + Spark Structured Streaming + Isolation Forest")

if not os.path.exists(CLEAN_PATH):
    st.warning("⏳ En attente de données... Lance le producer et Spark.")
    time.sleep(3)
    st.rerun()

df     = pd.read_csv(CLEAN_PATH)
df["timestamp"] = pd.to_datetime(df["timestamp"])
latest = df.sort_values("timestamp").groupby("coin").last().reset_index()

# ── Last update ───────────────────────────
st.success(f"🔄 {pd.Timestamp.now().strftime('%H:%M:%S')} — {len(df)} events")

# ── KPIs ──────────────────────────────────
cols = st.columns(len(latest))
for i, row in latest.iterrows():
    cols[i].metric(
        label=f"🪙 {row['coin'].upper()}",
        value=f"${row['price_usd']:,.2f}",
        delta=f"{row['change_24h']:+.2f}%"
    )

st.divider()

# ── Charts ────────────────────────────────
c1, c2 = st.columns(2)
with c1:
    st.subheader("💰 Prix en temps réel")
    st.plotly_chart(
        px.line(df.sort_values("timestamp"),
                x="timestamp", y="price_usd", color="coin"),
        use_container_width=True
    )
with c2:
    st.subheader("📊 Variation 24h")
    st.plotly_chart(
        px.bar(latest, x="coin", y="change_24h",
               color="change_24h", color_continuous_scale="RdYlGn",
               text="change_24h"),
        use_container_width=True
    )

c3, c4 = st.columns(2)
with c3:
    st.subheader("🚨 Anomaly Score")
    st.plotly_chart(
        px.scatter(df.sort_values("timestamp"),
                   x="timestamp", y="anomaly_score",
                   color="coin", symbol="is_anomaly"),
        use_container_width=True
    )
with c4:
    st.subheader("📦 Volume 24h")
    st.plotly_chart(
        px.bar(latest, x="coin", y="volume_24h", color="coin", log_y=True),
        use_container_width=True
    )

# ── Anomalies ─────────────────────────────
st.divider()
st.subheader("🚨 Alertes Anomalies")
anomalies = df[df["is_anomaly"] == 1].sort_values("timestamp", ascending=False)
if not anomalies.empty:
    st.error(f"⚠️ {len(anomalies)} anomalies détectées")
    st.dataframe(
        anomalies[["timestamp","coin","price_usd","change_24h","anomaly_score"]].head(20),
        use_container_width=True
    )
else:
    st.success("✅ Aucune anomalie détectée")

# ── Auto refresh ──────────────────────────
time.sleep(3)
st.rerun()