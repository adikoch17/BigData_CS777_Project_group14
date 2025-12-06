import html
import json
import os
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

import altair as alt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from kafka import KafkaConsumer
from wordcloud import STOPWORDS, WordCloud

# Optional .env override, otherwise defaults below are used
load_dotenv()

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PREDICTION = ROOT / "data" / "sample" / "Roberta_Prediction.csv"

bootstrap_raw = os.getenv("KAFKA_BOOTSTRAP", "3.236.215.110:9092")
BOOTSTRAP_SERVERS = [s.strip() for s in bootstrap_raw.split(",") if s.strip()]
TOPIC = os.getenv("KAFKA_PRED_TOPIC", "pred-stream")
GROUP_ID = os.getenv("KAFKA_PRED_GROUP_ID", "pred-stream-reader")
MAX_MESSAGES = 500  # Keep the feed from growing unbounded in the UI

PREDICTION_CSV_PATH = os.getenv("PREDICTION_CSV_PATH", str(DEFAULT_PREDICTION))

EMOJI_MAP: Dict[str, str] = {
    "joy": "😄",
    "anger": "😡",
    "sadness": "😢",
    "fear": "😨",
    "surprise": "😲",
    "neutral": "😐",
}
DEFAULT_EMOJI = "❓"
EMOTION_COLORS: Dict[str, str] = {
    "joy": "#4CAF50",
    "anger": "#E53935",
    "sadness": "#1E88E5",
    "fear": "#8E24AA",
    "surprise": "#FB8C00",
    "neutral": "#9E9E9E",
    "Unknown": "#607D8B",
}

LABEL_TO_EMOTION: Dict[int, str] = {
    0: "joy",
    1: "anger",
    2: "sadness",
    3: "fear",
    4: "surprise",
    5: "neutral",
}

try:  # Streamlit 1.18+ provides cache_resource
    cache_resource = st.cache_resource
except AttributeError:
    cache_resource = st.experimental_singleton


@cache_resource(show_spinner=False)
def get_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=GROUP_ID,
    )


def consume_batch(consumer: KafkaConsumer, timeout_ms: int = 500) -> List[Dict[str, Any]]:
    """Poll for a batch of messages without blocking the UI thread too long."""
    records = consumer.poll(timeout_ms=timeout_ms, max_records=50)
    events: List[Dict[str, Any]] = []
    for _, messages in records.items():
        for message in messages:
            payload = message.value if isinstance(message.value, dict) else {}
            text = payload.get("text", message.value if not payload else "")
            subreddit = payload.get("subreddit", "")
            score = payload.get("score", "")
            emotion = payload.get("emotion", "")
            events.append(
                {
                    "emotion": str(emotion),
                    "score": score,
                    "subreddit": str(subreddit) if subreddit is not None else "",
                    "text": str(text) if text is not None else "",
                    "partition": message.partition,
                    "offset": message.offset,
                }
            )
    return events


@st.cache_data(show_spinner=False)
def load_prediction_csv(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    df["emotion"] = df["pred_label"].map(LABEL_TO_EMOTION).fillna("Unknown")
    # Clean created for time-based charts
    if "created" in df.columns:
        df["created_dt"] = pd.to_datetime(df["created"], errors="coerce")
    else:
        df["created_dt"] = pd.NaT
    df["text_len"] = df["text"].fillna("").str.len()
    return df


def emotion_emoji(emotion: str) -> str:
    key = emotion.lower().strip()
    return EMOJI_MAP.get(key, DEFAULT_EMOJI) if key else DEFAULT_EMOJI


def render_card(entry: Dict[str, Any]):
    """Render a single message as a card with emoji, meta, and text."""
    emoji = emotion_emoji(entry.get("emotion", ""))
    emotion_label = entry.get("emotion", "") or "Unknown"
    subreddit = entry.get("subreddit", "")
    text = html.escape(entry.get("text", ""))
    score = entry.get("score", "")
    score_display = f"{score:.3f}" if isinstance(score, (int, float)) else str(score)

    st.markdown(
        f"""
        <div style="border:1px solid #e8e8e8;padding:12px 16px;border-radius:12px;background:#ffffff;margin-bottom:10px;box-shadow:0 1px 3px rgba(0,0,0,0.05);">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
                <div style="font-size:22px;font-weight:600;">{emoji} {emotion_label}</div>
                <div style="color:#555;font-size:13px;">score: {score_display}</div>
            </div>
            <div style="color:#777;font-size:13px;margin-bottom:8px;">subreddit: {html.escape(subreddit) if subreddit else "n/a"}</div>
            <div style="font-size:15px;line-height:1.45;color:#222;">{text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def main():
    st.set_page_config(page_title="Prediction Stream", page_icon="📡", layout="wide")
    st.title("Prediction Stream Dashboard")

    page = st.sidebar.radio(
        "View",
        ["Live Stream", "Roberta Prediction Explorer"],
        index=0,
        help="Switch between the live Kafka stream and the saved Roberta predictions table.",
    )

    if page == "Live Stream":
        st.caption(f"Listening to '{TOPIC}' on {BOOTSTRAP_SERVERS}")

        status_box = st.sidebar.empty()
        if "messages" not in st.session_state:
            st.session_state.messages: List[Dict[str, Any]] = []

        if st.sidebar.button("Clear feed"):
            st.session_state.messages = []
            st.experimental_rerun()

        consumer = get_consumer()
        chart_placeholder = st.empty()
        wordcloud_placeholder = st.empty()
        feed_placeholder = st.empty()

        try:
            while True:
                new_events = consume_batch(consumer)
                if new_events:
                    st.session_state.messages.extend(new_events)
                    if len(st.session_state.messages) > MAX_MESSAGES:
                        st.session_state.messages = st.session_state.messages[-MAX_MESSAGES:]

                if st.session_state.messages:
                    window = st.session_state.messages[-200:]
                    counts = Counter(
                        (entry.get("emotion") or "Unknown").strip() or "Unknown"
                        for entry in window
                    )
                    df = (
                        pd.DataFrame(
                            {"emotion": list(counts.keys()), "count": list(counts.values())}
                        )
                        .sort_values("count", ascending=False)
                    )

                    chart = (
                        alt.Chart(df)
                        .mark_bar()
                        .encode(
                            x=alt.X("emotion:N", sort="-y", title="Emotion"),
                            y=alt.Y("count:Q", title="Count"),
                            color=alt.Color(
                                "emotion:N",
                                title="Emotion",
                                scale=alt.Scale(
                                    domain=list(EMOTION_COLORS.keys()),
                                    range=list(EMOTION_COLORS.values()),
                                ),
                            ),
                            tooltip=["emotion", "count"],
                        )
                        .properties(width="container", height=320)
                    )
                    chart_placeholder.altair_chart(chart, use_container_width=True)

                    recent_texts = [
                        entry.get("text", "") or "" for entry in st.session_state.messages[-100:]
                    ]
                    combined_text = " ".join(recent_texts).strip()
                    if combined_text:
                        wc = WordCloud(
                            width=1000,
                            height=400,
                            background_color="white",
                            stopwords=STOPWORDS,
                            max_words=200,
                        ).generate(combined_text)
                        wordcloud_placeholder.image(wc.to_array(), use_column_width=True)
                    else:
                        wordcloud_placeholder.info(
                            "Waiting for text to build the word cloud..."
                        )
                else:
                    chart_placeholder.info("Waiting for messages to show emotion distribution...")
                    wordcloud_placeholder.info("Waiting for text to build the word cloud...")

                with feed_placeholder.container():
                    if st.session_state.messages:
                        for entry in reversed(st.session_state.messages):
                            render_card(entry)
                    else:
                        st.info("Waiting for messages...")

                status_box.caption(f"Last update: {time.strftime('%H:%M:%S')}")
                time.sleep(1.0)
        except KeyboardInterrupt:
            status_box.warning("Stopping prediction stream consumer...")
        finally:
            try:
                consumer.close()
            except Exception:
                pass
    else:
        st.caption(f"Exploring '{PREDICTION_CSV_PATH}'")
        df = load_prediction_csv(PREDICTION_CSV_PATH)
        if df is None:
            st.error(f"Could not find {PREDICTION_CSV_PATH}.")
            return

        # Filters
        subreddits = sorted([s for s in df["subreddit"].dropna().unique()])
        emotions = sorted(df["emotion"].dropna().unique())
        selected_subs = st.multiselect("Subreddit", subreddits, default=subreddits)
        selected_emotions = st.multiselect("Emotion", emotions, default=emotions)
        conf_min, conf_max = st.slider(
            "Predicted confidence",
            min_value=float(df["pred_confidence"].min()),
            max_value=float(df["pred_confidence"].max()),
            value=(
                float(df["pred_confidence"].min()),
                float(df["pred_confidence"].max()),
            ),
        )
        created_min = df["created_dt"].min()
        created_max = df["created_dt"].max()
        date_range = None
        if pd.notna(created_min) and pd.notna(created_max):
            date_range = st.date_input(
                "Created date range",
                (created_min.date(), created_max.date()),
            )
        text_query = st.text_input("Search text/title/content", "")

        filtered = df[
            df["subreddit"].isin(selected_subs)
            & df["emotion"].isin(selected_emotions)
            & df["pred_confidence"].between(conf_min, conf_max)
        ]
        if date_range and len(date_range) == 2:
            start_date, end_date = date_range
            if start_date and end_date:
                mask = filtered["created_dt"].dt.date.between(start_date, end_date)
                filtered = filtered[mask]
        if text_query:
            q = text_query.lower()
            filtered = filtered[
                filtered["text"].fillna("").str.lower().str.contains(q)
                | filtered["title"].fillna("").str.lower().str.contains(q)
                | filtered["content"].fillna("").str.lower().str.contains(q)
            ]

        c1, c2, c3 = st.columns(3)
        avg_conf = filtered["pred_confidence"].mean() if len(filtered) else 0.0
        avg_len = filtered["text_len"].mean() if len(filtered) else 0.0
        c1.metric("Rows", len(filtered))
        c2.metric("Avg confidence", f"{avg_conf:.3f}")
        c3.metric("Avg text length", f"{avg_len:.0f}")

        # Visuals
        col_chart, col_hist = st.columns([2, 1])
        with col_chart:
            bar_df = (
                filtered.groupby("emotion")["id"]
                .count()
                .reset_index()
                .rename(columns={"id": "count"})
            )
            if not bar_df.empty:
                bar_chart = (
                    alt.Chart(bar_df)
                    .mark_bar()
                    .encode(
                        x=alt.X("emotion:N", sort="-y", title="Emotion"),
                        y=alt.Y("count:Q", title="Count"),
                        color=alt.Color(
                            "emotion:N",
                            title="Emotion",
                            scale=alt.Scale(
                                domain=list(EMOTION_COLORS.keys()),
                                range=list(EMOTION_COLORS.values()),
                            ),
                        ),
                        tooltip=["emotion", "count"],
                    )
                    .properties(height=320)
                )
                st.altair_chart(bar_chart, use_container_width=True)
            else:
                st.info("No rows for current filters.")

        with col_hist:
            if len(filtered):
                hist = (
                    alt.Chart(filtered)
                    .mark_bar()
                    .encode(
                        x=alt.X(
                            "pred_confidence:Q", bin=alt.Bin(maxbins=20), title="Confidence"
                        ),
                        y=alt.Y("count()", title="Count"),
                        tooltip=[alt.Tooltip("count()", title="Count")],
                    )
                    .properties(height=320)
                )
                st.altair_chart(hist, use_container_width=True)
            else:
                st.info("No data for histogram.")

        if len(filtered) and filtered["created_dt"].notna().any():
            timeline = (
                alt.Chart(filtered.dropna(subset=["created_dt"]))
                .mark_line(point=True)
                .encode(
                    x=alt.X("created_dt:T", title="Created"),
                    y=alt.Y("count()", title="Posts"),
                    color=alt.Color("emotion:N", title="Emotion"),
                    tooltip=["emotion", alt.Tooltip("count()", title="Count"), "created_dt:T"],
                )
                .properties(height=280)
            )
            st.altair_chart(timeline, use_container_width=True)

        st.subheader("Word cloud (filtered)")
        wc_text = " ".join(filtered["text"].fillna("").tolist()).strip()
        if wc_text:
            wc = WordCloud(
                width=1000,
                height=400,
                background_color="white",
                stopwords=STOPWORDS,
                max_words=200,
            ).generate(wc_text)
            st.image(wc.to_array(), use_column_width=True)
        else:
            st.info("No text available for current filters.")

        st.subheader("Filtered table")
        st.dataframe(
            filtered[
                [
                    "id",
                    "subreddit",
                    "emotion",
                    "pred_confidence",
                    "title",
                    "content",
                    "text",
                ]
            ].reset_index(drop=True),
            use_container_width=True,
            height=400,
        )

        st.subheader("Top confident posts")
        top_n = filtered.sort_values("pred_confidence", ascending=False).head(5)
        for _, row in top_n.iterrows():
            with st.expander(f"{row['emotion']} • {row['pred_confidence']:.3f} • {row['title'] or 'No title'}"):
                st.write(f"**Subreddit:** {row['subreddit']}")
                st.write(f"**Text:** {row['text']}")
                if row.get("content"):
                    st.write(f"**Content:** {row['content']}")


if __name__ == "__main__":
    main()
