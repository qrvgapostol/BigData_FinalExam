"""
Streaming Data Dashboard
Kafka + MongoDB + Streamlit
"""

import json
import time
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh
import plotly.express as px

# Kafka defaults
DEFAULT_KAFKA_BROKER = "localhost:9093"
DEFAULT_TOPIC = "streaming-data"

# MongoDB config
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "bigdata_dashboard"
MONGO_COLLECTION = "records"

st.set_page_config(
    page_title="Streaming Data Dashboard",
    layout="wide",
)


# --------------- Mongo helpers ---------------

def get_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]


def save_rows_to_mongo(rows):
    """Insert a list of dict rows into Mongo."""
    if not rows:
        return
    col = get_collection()
    col.insert_many(rows)


def query_historical_data(time_range="1h", metric_type="all"):
    """Load historical data from Mongo within a time range."""
    col = get_collection()
    now = datetime.utcnow()

    if time_range == "1h":
        start = now - timedelta(hours=1)
    elif time_range == "24h":
        start = now - timedelta(hours=24)
    elif time_range == "7d":
        start = now - timedelta(days=7)
    else:
        start = now - timedelta(days=30)

    query = {"timestamp": {"$gte": start}}
    if metric_type != "all":
        query["metric_type"] = metric_type

    docs = list(col.find(query).sort("timestamp", 1))
    if not docs:
        return pd.DataFrame()

    df = pd.DataFrame(docs)

    # Drop MongoDB ObjectId so Streamlit and Arrow will not fail
    if "_id" in df.columns:
        df = df.drop(columns=["_id"])

    return df


# --------------- Kafka consumer ---------------

def consume_kafka_data(broker, topic, max_messages=50, timeout_sec=5):
    """
    Read up to max_messages from Kafka and return as DataFrame.

    Starts from the beginning of the topic (earliest) so we are
    guaranteed to see messages if the producer has ever sent any.
    Also saves everything into MongoDB for historical queries.
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[broker],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=None,  # new consumer each call
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
    except Exception as e:
        st.error(f"Kafka connection error: {e}")
        return pd.DataFrame()

    rows = []
    start_time = time.time()

    try:
        while time.time() - start_time < timeout_sec and len(rows) < max_messages:
            msg_pack = consumer.poll(timeout_ms=1000)
            if not msg_pack:
                continue

            for _, batch in msg_pack.items():
                for msg in batch:
                    data = msg.value
                    try:
                        ts_str = data.get("timestamp")
                        if ts_str is None:
                            continue
                        # handle optional trailing Z
                        if ts_str.endswith("Z"):
                            ts_str = ts_str[:-1]
                        ts = datetime.fromisoformat(ts_str)

                        row = {
                            "timestamp": ts,
                            "value": float(data.get("value")),
                            "metric_type": str(data.get("metric_type")),
                            "sensor_id": str(data.get("sensor_id")),
                            "source": str(data.get("source", "api")),
                        }
                        rows.append(row)
                    except Exception:
                        # skip bad messages
                        continue
    finally:
        consumer.close()

    if not rows:
        return pd.DataFrame()

    # Save to Mongo for history
    save_rows_to_mongo(rows)

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp")
    return df


# --------------- UI ---------------

st.title("Streaming Data Dashboard")

st.sidebar.title("Controls")
kafka_broker = st.sidebar.text_input("Kafka Broker", DEFAULT_KAFKA_BROKER)
kafka_topic = st.sidebar.text_input("Kafka Topic", DEFAULT_TOPIC)
refresh_interval = st.sidebar.slider("Auto refresh (seconds)", 5, 60, 15)

st_autorefresh(interval=refresh_interval * 1000, key="refresh")

tab_live, tab_hist = st.tabs(["Real time", "Historical"])


# Small utility: add change columns for nicer charts and metrics
def add_change_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values("timestamp")
    df["change"] = df["value"].diff()
    df["pct_change"] = df["value"].pct_change() * 100
    return df


# ----- Real time tab -----
with tab_live:
    st.header("Real time streaming view")

    with st.spinner("Reading data from Kafka..."):
        df_live = consume_kafka_data(kafka_broker, kafka_topic)

    if df_live.empty:
        st.info("No live data yet. Check that Kafka and producer.py are running.")
    else:
        df_live = add_change_columns(df_live)

        # Summary metrics
        latest = df_live.iloc[-1]
        prev_change = latest["change"] if pd.notna(latest["change"]) else 0.0
        prev_pct = latest["pct_change"] if pd.notna(latest["pct_change"]) else 0.0

        col_a, col_b, col_c = st.columns(3)
        col_a.metric(
            "Latest BTC price (USD)",
            f"{latest['value']:.2f}",
        )
        col_b.metric(
            "Change since last tick",
            f"{prev_change:+.2f}",
        )
        col_c.metric(
            "Change percent",
            f"{prev_pct:+.3f} %",
        )

        st.subheader("Latest messages")
        st.dataframe(df_live.tail(20), use_container_width=True)

        st.subheader("BTC price over time (USD)")
        price_series = df_live.set_index("timestamp")["value"]
        fig_price = px.line(
            price_series,
            labels={"timestamp": "Time", "value": "BTC price USD"},
        )
        fig_price.update_layout(height=400, xaxis_rangeslider_visible=True)
        st.plotly_chart(fig_price, use_container_width=True)

        st.subheader("Price change percent over time")
        pct_series = df_live.set_index("timestamp")["pct_change"].dropna()
        if pct_series.empty:
            st.write("Not enough data yet to compute percentage change.")
        else:
            fig_pct = px.line(
                pct_series,
                labels={"timestamp": "Time", "value": "Percent change"},
            )
            fig_pct.update_layout(height=300)
            st.plotly_chart(fig_pct, use_container_width=True)


# ----- Historical tab -----
with tab_hist:
    st.header("Historical data view")

    col1, col2 = st.columns(2)
    with col1:
        time_range = st.selectbox(
            "Time range",
            ["1h", "24h", "7d", "30d"],
            index=1,
        )
    with col2:
        metric_filter = st.selectbox(
            "Metric type",
            ["all", "BTC_USD"],
            index=1,
        )

    with st.spinner("Loading historical data from MongoDB..."):
        df_hist = query_historical_data(time_range=time_range, metric_type=metric_filter)

    if df_hist.empty:
        st.info("No historical data found yet.")
    else:
        df_hist = add_change_columns(df_hist)

        # Aggregate summary for the selected period
        period_min = df_hist["value"].min()
        period_max = df_hist["value"].max()
        period_avg = df_hist["value"].mean()

        col_h1, col_h2, col_h3 = st.columns(3)
        col_h1.metric("Minimum price in range", f"{period_min:.2f} USD")
        col_h2.metric("Maximum price in range", f"{period_max:.2f} USD")
        col_h3.metric("Average price in range", f"{period_avg:.2f} USD")

        st.subheader("Records")
        st.dataframe(df_hist, use_container_width=True)

        st.subheader("Trend")
        hist_price_series = df_hist.set_index("timestamp")["value"]
        fig_hist = px.line(
            hist_price_series,
            labels={"timestamp": "Time", "value": "BTC price USD"},
        )
        fig_hist.update_layout(height=400, xaxis_rangeslider_visible=True)
        st.plotly_chart(fig_hist, use_container_width=True)

        st.subheader("Percent change trend")
        hist_pct_series = df_hist.set_index("timestamp")["pct_change"].dropna()
        if not hist_pct_series.empty:
            fig_hist_pct = px.line(
                hist_pct_series,
                labels={"timestamp": "Time", "value": "Percent change"},
            )
            fig_hist_pct.update_layout(height=300)
            st.plotly_chart(fig_hist_pct, use_container_width=True)

        st.subheader("Export")
        csv = df_hist.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            csv,
            "historical_data.csv",
            "text/csv",
        )
