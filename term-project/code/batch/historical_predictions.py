"""
Batch inference script for historical Reddit posts.

- Reads a cleaned CSV (columns: subreddit, created, title, content, text)
- Runs a fine-tuned 6-class emotion model (GoEmotions variant)
- Writes predictions with label id, emotion name, and confidence.
"""

import argparse
import os
from pathlib import Path
from typing import List, Sequence, Tuple

import pandas as pd
import torch
from dotenv import load_dotenv
from transformers import AutoModelForSequenceClassification, AutoTokenizer

load_dotenv()

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = ROOT / "data" / "sample" / "clean.csv"
DEFAULT_OUTPUT = ROOT / "data" / "sample" / "Roberta_Prediction.csv"
DEFAULT_MODEL = ROOT / "models" / "roberta-goemotions-6class-final"

SIX_LABELS = ["joy", "anger", "sadness", "fear", "surprise", "neutral"]


def build_text(row: pd.Series) -> str:
    """Merge title + content when text is missing."""
    if pd.notna(row.get("text")) and str(row.get("text")).strip():
        return " ".join(str(row.get("text")).split())
    title = str(row.get("title", "") or "").strip()
    content = str(row.get("content", "") or "").strip()
    merged = f"{title} {content}".strip()
    return " ".join(merged.split())


def load_model(model_dir: Path):
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    model = AutoModelForSequenceClassification.from_pretrained(model_dir)
    id2label = {int(k): v for k, v in getattr(model.config, "id2label", {}).items()}
    if not id2label:
        id2label = {i: lab for i, lab in enumerate(SIX_LABELS)}
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    model.eval()
    return tokenizer, model, id2label, device


def predict_batch(
    tokenizer,
    model,
    device,
    texts: Sequence[str],
    max_length: int = 160,
) -> Tuple[List[int], List[float]]:
    inputs = tokenizer(
        list(texts),
        return_tensors="pt",
        truncation=True,
        padding=True,
        max_length=max_length,
    ).to(device)

    with torch.no_grad():
        logits = model(**inputs).logits
        probs = torch.softmax(logits, dim=-1)
        confs, pred_ids = torch.max(probs, dim=-1)

    return pred_ids.cpu().tolist(), confs.cpu().tolist()


def run_inference(
    input_csv: Path,
    output_csv: Path,
    model_dir: Path,
    batch_size: int = 16,
):
    tokenizer, model, id2label, device = load_model(model_dir)
    df = pd.read_csv(input_csv)

    if "text" not in df.columns:
        df["text"] = ""
    df["text"] = df.apply(build_text, axis=1)

    pred_labels: List[int] = []
    pred_conf: List[float] = []

    for start in range(0, len(df), batch_size):
        end = start + batch_size
        batch_texts = df.iloc[start:end]["text"].fillna("").tolist()
        labels, confs = predict_batch(tokenizer, model, device, batch_texts)
        pred_labels.extend(labels)
        pred_conf.extend([float(c) for c in confs])

    df["pred_label"] = pred_labels
    df["pred_emotion"] = [id2label.get(lbl, "Unknown") for lbl in pred_labels]
    df["pred_confidence"] = pred_conf
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    print(f"Wrote {len(df)} rows with predictions to {output_csv}")


def parse_args():
    parser = argparse.ArgumentParser(description="Run historical emotion inference.")
    parser.add_argument(
        "--input",
        default=os.getenv("HIST_INPUT_CSV", DEFAULT_INPUT),
        type=Path,
        help="Cleaned CSV with columns like title/content/text.",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("HIST_OUTPUT_CSV", DEFAULT_OUTPUT),
        type=Path,
        help="Where to write the predictions CSV.",
    )
    parser.add_argument(
        "--model-dir",
        default=os.getenv("MODEL_DIR", DEFAULT_MODEL),
        type=Path,
        help="Directory containing the fine-tuned model.",
    )
    parser.add_argument(
        "--batch-size",
        default=int(os.getenv("HIST_BATCH_SIZE", 16)),
        type=int,
        help="Batch size for transformer inference.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_inference(args.input, args.output, args.model_dir, batch_size=args.batch_size)
