import json
import os
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA = ROOT / "data" / "sample" / "clean.csv"
DATA_PATH = Path(os.getenv("PRODUCER_CSV", DEFAULT_DATA))

bootstrap_raw = os.getenv("KAFKA_BOOTSTRAP", "3.236.215.110:9092")
BOOTSTRAP_SERVERS = [s.strip() for s in bootstrap_raw.split(",") if s.strip()]
TOPIC = os.getenv("KAFKA_TOPIC", "demo-stream")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

df = pd.read_csv(DATA_PATH)

df = df.sample(frac=1).reset_index(drop=True)
df['content'] = df['content'].str.replace('[removed]', '', regex=False)

for index, row in df.iterrows():
    title = "" if pd.isna(row["title"]) else str(row["title"])
    content = "" if pd.isna(row["content"]) else str(row["content"])
    subreddit = "" if pd.isna(row.get("subreddit", "")) else str(row.get("subreddit", ""))
    created = "" if pd.isna(row.get("created", "")) else str(row.get("created", ""))

    data = {
        "index": int(index),
        "subreddit": subreddit,
        "created": created,
        "title": title,
        "content": content,
    }

    producer.send(TOPIC, data)
    producer.flush()
    print("Sent JSON:", data)
    time.sleep(3)

producer.close()
