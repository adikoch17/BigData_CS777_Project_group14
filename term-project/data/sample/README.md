# Sample Data (Clean + Predictions)

Lightweight slices of the Reddit city corpus for quick demos, streaming dry-runs, or dashboard previews. Full raw pulls were ~45 GB (Pushshift top-40k subreddits) filtered to 5M+ rows; these are much smaller CSV extracts kept under version control for convenience.

## Files in this folder
- `clean.csv` — 119 MB, ~446k rows (newline-escaped content can inflate `wc -l`). Time range: 2024-01-01 to 2024-12-31 UTC. Columns: `subreddit, created, title, content`.
- `Roberta_Prediction.csv` — 50 MB, ~110k rows. Time range: 2008-03-24 to 2024-12-31 UTC. Columns: `id, subreddit, created, title, content, text, pred_label, pred_confidence`.

If you need to re-download/refresh, grab the originals:
- `clean.csv`: https://drive.google.com/file/d/17tbiOlgPdSBbBjahFUb8NKi-FCUFv3rS/view?usp=sharing
- `roberta_predictions.csv`: https://drive.google.com/file/d/1zRnDvsiLNNYQXvg2txd6U-g8Y5XySxSK/view?usp=sharing

## Schema details
- `subreddit` — city subreddit (Austin, Boston, Chicago, Dallas, Houston, LosAngeles, Miami, NYC, Orlando, Philadelphia, Phoenix, San Diego, San Francisco, Seattle, WashingtonDC).
- `created` — UTC timestamp (`yyyy-MM-dd HH:mm:ss`).
- `title` — Reddit submission title.
- `content` — submission body (`selftext`), may be empty or contain `[removed]`; producer strips `[removed]` before streaming.
- `text` — only in `Roberta_Prediction.csv`; combined title+content used for inference (whitespace-compressed).
- `pred_label` — integer 0–5 mapped to emotions: `0=joy, 1=anger, 2=sadness, 3=fear, 4=surprise, 5=neutral`.
- `pred_confidence` — model softmax probability for `pred_label` (float).
- `id` — row index from the source sample (not a Reddit ID).

## Usage
- **Streaming demo**: `producer.py` defaults to `clean.csv` and strips `[removed]` content before sending to Kafka.
- **Dashboard explorer**: point the Streamlit app to `Roberta_Prediction.csv` to browse historical predictions without Kafka.
- **Batch regeneration**: to rebuild predictions with your own model or add `pred_emotion`, run:
  ```
  python term-project/code/batch/historical_predictions.py \
    --input term-project/data/sample/clean.csv \
    --output term-project/data/sample/Roberta_Prediction.csv \
    --model-dir /path/to/roberta-goemotions-6class-final
  ```

## Notes
- Newlines inside quoted `content` fields mean `wc -l` overcounts rows; rely on pandas/CSV readers for row counts.
- Data is already lightly cleaned (lowercasing, URL/punct/stopword removal applied upstream for larger corpora). Title/body text remains user-generated; handle toxicity/PII appropriately in downstream use. 
