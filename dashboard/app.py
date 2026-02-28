
import sqlite3
import time
import os

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots


DB_PATH       = "../data/crypto_data.db"
REFRESH_SEC   = 10
COINS         = ["BTC", "ETH", "SOL", "ADA"]

COIN_COLORS = {
    "BTC": "#F7931A",
    "ETH": "#627EEA",
    "SOL": "#9945FF",
    "ADA": "#0033AD",
}

COIN_NAMES = {
    "BTC": "Bitcoin",
    "ETH": "Ethereum",
    "SOL": "Solana",
    "ADA": "Cardano",
}

st.set_page_config(
    page_title="Crypto Streaming Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e1e2e, #2a2a3e);
        border-radius: 12px;
        padding: 20px;
        border: 1px solid #3a3a5c;
        text-align: center;
    }
    .coin-price { font-size: 1.8rem; font-weight: bold; }
    .coin-name  { color: #aaa; font-size: 0.9rem; }
    .positive   { color: #00d4aa; }
    .negative   { color: #ff4d6d; }
    .neutral    { color: #aaaaaa; }
    div[data-testid="metric-container"] {
        background: #1e1e2e;
        border: 1px solid #3a3a5c;
        border-radius: 10px;
        padding: 12px;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_conn():
    """Persistent connection cached for the session."""
    if not os.path.exists(DB_PATH):
        st.error(f"Database not found at {DB_PATH}. Is the consumer running?")
        st.stop()
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def load_latest_prices(conn) -> pd.DataFrame:
    query = """
        SELECT symbol, price_usd, change_24h, volume_24h, market_cap, timestamp
        FROM raw_prices
        WHERE (symbol, ingested_at) IN (
            SELECT symbol, MAX(ingested_at)
            FROM raw_prices
            GROUP BY symbol
        )
        ORDER BY symbol
    """
    try:
        return pd.read_sql_query(query, conn)
    except Exception:
        return pd.DataFrame()


def load_price_history(conn, symbol: str, limit: int = 100) -> pd.DataFrame:
    query = f"""
        SELECT price_usd, timestamp, ingested_at
        FROM raw_prices
        WHERE symbol = '{symbol}'
        ORDER BY ingested_at DESC
        LIMIT {limit}
    """
    try:
        df = pd.read_sql_query(query, conn)
        return df.iloc[::-1].reset_index(drop=True)  # reverse to chronological
    except Exception:
        return pd.DataFrame()


def load_aggregations(conn, limit: int = 200) -> pd.DataFrame:
    query = f"""
        SELECT *
        FROM aggregations
        ORDER BY ingested_at DESC
        LIMIT {limit}
    """
    try:
        df = pd.read_sql_query(query, conn)
        return df.iloc[::-1].reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def load_latest_predictions(conn) -> pd.DataFrame:
    query = """
        SELECT symbol, current_price, predicted_price, delta_pct, r2_score,
               history_size, timestamp
        FROM predictions
        WHERE (symbol, ingested_at) IN (
            SELECT symbol, MAX(ingested_at)
            FROM predictions
            GROUP BY symbol
        )
        ORDER BY symbol
    """
    try:
        return pd.read_sql_query(query, conn)
    except Exception:
        return pd.DataFrame()


def load_prediction_history(conn, limit: int = 50) -> pd.DataFrame:
    query = f"""
        SELECT symbol, predicted_price, current_price, delta_pct, r2_score, ingested_at
        FROM predictions
        ORDER BY ingested_at DESC
        LIMIT {limit}
    """
    try:
        df = pd.read_sql_query(query, conn)
        return df.iloc[::-1].reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

def price_history_chart(conn) -> go.Figure:
    fig = go.Figure()
    for symbol in COINS:
        df = load_price_history(conn, symbol)
        if df.empty:
            continue
        fig.add_trace(go.Scatter(
            x=df["ingested_at"],
            y=df["price_usd"],
            name=f"{symbol}",
            line=dict(color=COIN_COLORS[symbol], width=2),
            mode="lines+markers",
            marker=dict(size=4),
        ))
    fig.update_layout(
        title="💹 Live Price History",
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        template="plotly_dark",
        height=420,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=0, r=0, t=50, b=0),
    )
    return fig


def volatility_chart(agg_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if agg_df.empty:
        return fig
    for symbol in COINS:
        sub = agg_df[agg_df["symbol"] == symbol].tail(30)
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub["window_start"],
            y=sub["price_volatility"],
            name=symbol,
            fill="tozeroy",
            line=dict(color=COIN_COLORS[symbol], width=1.5),
            mode="lines",
        ))
    fig.update_layout(
        title="📊 Price Volatility (Std Dev per Window)",
        xaxis_title="Window Start",
        yaxis_title="Volatility (σ)",
        template="plotly_dark",
        height=320,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=0, r=0, t=50, b=0),
    )
    return fig


def avg_price_bar(agg_df: pd.DataFrame) -> go.Figure:
    if agg_df.empty:
        return go.Figure()
    latest = agg_df.groupby("symbol").last().reset_index()
    fig = go.Figure(go.Bar(
        x=latest["symbol"],
        y=latest["avg_price"],
        marker_color=[COIN_COLORS.get(s, "#888") for s in latest["symbol"]],
        text=latest["avg_price"].apply(lambda x: f"${x:,.2f}"),
        textposition="outside",
    ))
    fig.update_layout(
        title="📉 Latest Avg Price per Coin (Last Window)",
        template="plotly_dark",
        height=320,
        yaxis_title="Avg Price (USD)",
        margin=dict(l=0, r=0, t=50, b=0),
    )
    return fig


def prediction_vs_actual_chart(conn) -> go.Figure:
    df = load_prediction_history(conn)
    if df.empty:
        return go.Figure()

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[f"{COIN_NAMES[s]} ({s})" for s in COINS],
    )
    positions = {COINS[0]: (1,1), COINS[1]: (1,2), COINS[2]: (2,1), COINS[3]: (2,2)}

    for symbol in COINS:
        sub  = df[df["symbol"] == symbol]
        row, col = positions[symbol]
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub["ingested_at"], y=sub["current_price"],
            name=f"{symbol} Actual",
            line=dict(color=COIN_COLORS[symbol], width=2),
            mode="lines",
        ), row=row, col=col)
        fig.add_trace(go.Scatter(
            x=sub["ingested_at"], y=sub["predicted_price"],
            name=f"{symbol} Predicted",
            line=dict(color=COIN_COLORS[symbol], width=2, dash="dot"),
            mode="lines",
        ), row=row, col=col)

    fig.update_layout(
        title_text="🤖 Actual vs Predicted Prices",
        template="plotly_dark",
        height=500,
        showlegend=False,
        margin=dict(l=0, r=0, t=60, b=0),
    )
    return fig

def main():
    conn = get_conn()

    # ── Sidebar ────────────────────────────────────────────────────────────
    with st.sidebar:
        st.image("https://cryptologos.cc/logos/bitcoin-btc-logo.png", width=60)
        st.title("Crypto Streaming")
        st.markdown("---")
        st.markdown("**Pipeline Status**")
        st.success("🟢 Kafka Connected")
        st.info(f"🔄 Refresh every {REFRESH_SEC}s")
        st.markdown("---")
        st.markdown("**Coins Tracked**")
        for sym, name in COIN_NAMES.items():
            st.markdown(f"● {name} ({sym})")
        st.markdown("---")
        st.caption("Powered by CoinGecko API + Apache Kafka + Spark Structured Streaming")

    # ── Title ──────────────────────────────────────────────────────────────
    st.title("📈 Real-Time Crypto Streaming Dashboard")
    st.caption(f"Last updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')} · Auto-refreshes every {REFRESH_SEC}s")

    # ── Load data ──────────────────────────────────────────────────────────
    latest_prices = load_latest_prices(conn)
    agg_df        = load_aggregations(conn)
    pred_df       = load_latest_predictions(conn)

    # ── Section 1: Price KPI cards ─────────────────────────────────────────
    st.markdown("## 💰 Current Prices")
    kpi_cols = st.columns(4)

    for i, symbol in enumerate(COINS):
        row = latest_prices[latest_prices["symbol"] == symbol]
        with kpi_cols[i]:
            if row.empty:
                st.metric(label=f"{symbol}", value="—", delta="No data yet")
            else:
                price    = row.iloc[0]["price_usd"]
                change   = row.iloc[0]["change_24h"]
                delta_str = f"{change:+.2f}% (24h)" if pd.notna(change) else "—"
                st.metric(
                    label=f"{COIN_NAMES[symbol]} ({symbol})",
                    value=f"${price:,.2f}",
                    delta=delta_str,
                    delta_color="normal",
                )

    # ── Section 2: Price history chart ────────────────────────────────────
    st.markdown("---")
    st.plotly_chart(price_history_chart(conn), use_container_width=True)

    # ── Section 3: Aggregations ───────────────────────────────────────────
    st.markdown("---")
    st.markdown("## 📊 Windowed Aggregations")

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(avg_price_bar(agg_df), use_container_width=True)
    with col2:
        st.plotly_chart(volatility_chart(agg_df), use_container_width=True)

    if not agg_df.empty:
        st.markdown("**Latest Aggregation Windows**")
        display_agg = agg_df[["window_start", "symbol", "avg_price", "min_price",
                               "max_price", "price_volatility", "event_count"]].tail(20)
        st.dataframe(display_agg[::-1], use_container_width=True, hide_index=True)

    # ── Section 4: ML Predictions ─────────────────────────────────────────
    st.markdown("---")
    st.markdown("## 🤖 ML Price Predictions (Linear Regression)")

    pred_cols = st.columns(4)
    for i, symbol in enumerate(COINS):
        row = pred_df[pred_df["symbol"] == symbol] if not pred_df.empty else pd.DataFrame()
        with pred_cols[i]:
            if row.empty:
                st.metric(
                    label=f"🔮 {symbol} Next Price",
                    value="Building model...",
                    delta="Need more data",
                )
            else:
                r = row.iloc[0]
                delta_str = f"{r['delta_pct']:+.2f}%"
                st.metric(
                    label=f"🔮 {symbol} Next Price",
                    value=f"${r['predicted_price']:,.2f}",
                    delta=delta_str,
                    delta_color="normal",
                )
                st.caption(f"R² = {r['r2_score']:.4f} · n={int(r['history_size'])}")

    st.plotly_chart(prediction_vs_actual_chart(conn), use_container_width=True)

    time.sleep(REFRESH_SEC)
    st.rerun()


if __name__ == "__main__":
    main()