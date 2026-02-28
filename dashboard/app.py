import streamlit as st
import pandas as pd
import plotly.express as px
import os, time

st.set_page_config(page_title="📈 Crypto Streaming", layout="wide")

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
CLEAN_PATH   = os.path.join(BASE_DIR, "..", "output", "crypto_clean.csv")
PRED_PATH    = os.path.join(BASE_DIR, "..", "output", "crypto_predictions.csv")
ANOMALY_PATH = os.path.join(BASE_DIR, "..", "output", "crypto_anomalies.csv")

st.title("📈 Real-Time Crypto Streaming Dashboard")
st.caption("Kafka + Spark Structured Streaming + Z-Score Anomaly Detection + Linear Regression")

if not os.path.exists(CLEAN_PATH):
    st.warning("⏳ En attente de données... Lance le producer et Spark.")
    time.sleep(3)
    st.experimental_rerun()

# ── Load data ─────────────────────────────
df = pd.read_csv(CLEAN_PATH)
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df["timestamp"] = df["timestamp"].dt.tz_localize("UTC").dt.tz_convert("Africa/Tunis")  # ✅
df["timestamp"] = df["timestamp"].dt.tz_localize(None)  # ✅ enlève tz pour plotly
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

# ── Row 1 : Prix (gauche) + Variation & Bubble (droite) ──
c1, c2 = st.columns([4, 3])  # ✅ gauche plus large

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
                height=800  # ✅ hauteur totale
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
    # ✅ Graphe 1 — Variation 24h (moitié haute)
    st.subheader("📊 Variation 24h")
    fig2 = px.bar(
        latest.sort_values("change_24h"),
        x="coin", y="change_24h",
        color="change_24h",
        color_continuous_scale="RdYlGn",
        text="change_24h",
        height=390  # ✅ moitié de 800
    )
    fig2.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig2.update_layout(height=390, margin=dict(t=30, b=10))
    st.plotly_chart(fig2, use_container_width=True)

    # ✅ Graphe 2 — Bubble volume (moitié basse)
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
        height=390  # ✅ moitié de 800
    )
    fig_bubble.update_traces(textposition="top center")
    fig_bubble.update_layout(height=390, showlegend=False, margin=dict(t=30, b=10))
    st.plotly_chart(fig_bubble, use_container_width=True)
    
# ── Row 2 ─────────────────────────────────
c3, c4 = st.columns([4, 3])

with c3:
    st.subheader("🚨 Z-Score Anomaly Over Time")
    # ✅ Vérifier que la colonne existe
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
if os.path.exists(PRED_PATH):
    df_pred = pd.read_csv(PRED_PATH)
    # Garder dernière prédiction par coin
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

if os.path.exists(ANOMALY_PATH):
    df_anomaly = pd.read_csv(ANOMALY_PATH)
    real_anomalies = df_anomaly[df_anomaly["status"] == "ANOMALIE"]
    if not real_anomalies.empty:
        st.error(f"⚠️ {len(real_anomalies)} anomalies détectées !")
        st.dataframe(real_anomalies.tail(20), use_container_width=True)
    else:
        st.success("✅ Aucune anomalie détectée pour l'instant")
else:
    st.success("✅ Aucune anomalie détectée pour l'instant")

# ── Raw data ──────────────────────────────
with st.expander("📋 Données brutes (50 derniers events)"):
    st.dataframe(
        df.sort_values("timestamp", ascending=False).head(50),
        use_container_width=True
    )

# ── Auto refresh ──────────────────────────
time.sleep(10)
st.experimental_rerun()