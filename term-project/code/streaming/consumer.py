import json
import os
import re
from pathlib import Path

import torch
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from transformers import AutoModelForSequenceClassification, AutoTokenizer

# Load environment variables from .env (bootstrap server, topic, etc.)
load_dotenv()

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MODEL_DIR = ROOT / "models" / "roberta-goemotions-6class-final"

bootstrap_raw = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
BOOTSTRAP_SERVERS = [s.strip() for s in bootstrap_raw.split(",") if s.strip()]
TOPIC = os.getenv("KAFKA_TOPIC", "demo-stream")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "emotion-consumer")
MODEL_DIR = Path(os.getenv("MODEL_DIR", DEFAULT_MODEL_DIR))
PRED_TOPIC = os.getenv("KAFKA_PRED_TOPIC", "pred-stream")

# Load tokenizer/model once at startup
tokenizer = AutoTokenizer.from_pretrained(MODEL_DIR)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_DIR)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)
model.eval()

# Label mapping (falls back to default 6-class order if not stored in config)
six_labels = ["joy", "anger", "sadness", "fear", "surprise", "neutral"]
id2six_label = {
    int(k): v for k, v in getattr(model.config, "id2label", {}).items()
} or {i: lab for i, lab in enumerate(six_labels)}


def predict_emotion_6(text: str):
    inputs = tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        max_length=128,
    ).to(device)

    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.softmax(outputs.logits, dim=-1)[0]
        pred_id = int(torch.argmax(probs).item())

    return id2six_label[pred_id], float(probs[pred_id].item())


def extract_text(value):
    """
    Accepts either a dict (with title/content) or a plain string payload.
    """
    if isinstance(value, dict):
        title = str(value.get("title", ""))
        content = str(value.get("content", ""))
        combined = f"{title} {content}".strip()
        if combined:
            return " ".join(combined.split())
    return " ".join(str(value).split())


_url_re = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_mention_re = re.compile(r"@[A-Za-z0-9_]+")
_hashtag_re = re.compile(r"#[A-Za-z0-9_]+")


def clean_text(text: str) -> str:
    """Light cleanup: strip URLs, mentions, hashtags, and compress whitespace."""
    text = _url_re.sub(" ", text)
    text = _mention_re.sub(" ", text)
    text = _hashtag_re.sub(" ", text)
    text = " ".join(text.split())
    return text.strip()


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=GROUP_ID,
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Connected to {BOOTSTRAP_SERVERS}. Waiting for messages on topic '{TOPIC}'...")

for message in consumer:
    payload = message.value
    subreddit = ""
    if isinstance(payload, dict):
        raw_sub = payload.get("subreddit", "")
        subreddit = str(raw_sub) if raw_sub is not None else ""
    text = clean_text(extract_text(payload))
    emotion, score = predict_emotion_6(text)
    preview = text if len(text) <= 200 else text[:200] + "..."
    print(
        f"[{message.partition}:{message.offset}] emotion={emotion} "
        f"score={score:.3f} subreddit={subreddit} text={preview}"
    )
    producer.send(
        PRED_TOPIC,
        {
            "emotion": emotion,
            "score": score,
            "text": text,
            "subreddit": subreddit,
            "source_topic": TOPIC,
            "partition": message.partition,
            "offset": message.offset,
        },
    )
