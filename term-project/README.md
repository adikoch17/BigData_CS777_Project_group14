# Reddit City Emotion Pipeline (Term Project)

Self-contained copy of the Reddit city-level emotion pipeline: Spark cleaning of Pushshift dumps, RoBERTa/GoEmotions inference, Kafka streaming, and a Streamlit dashboard.

## Quickstart
1) Install deps from repo root: `pip install -r requirements.txt`
2) Fetch raw NDJSON per city (see “Data Sources”) into `term-project/data/city_submissions/`.
3) Run Spark cleaning (writes per-city CSV + 2024 aggregate): `spark-submit term-project/code/spark/data_cleaning_spark.py`
4) Optional batch inference on cleaned CSVs: `python term-project/code/batch/historical_predictions.py ...`
5) Bring up Kafka (topics `demo-stream`, `pred-stream`), then start:
   - Producer: `python term-project/code/streaming/producer.py`
   - Consumer/ML: `python term-project/code/streaming/consumer.py`
6) Dashboard: `streamlit run term-project/code/dashboard/pred_stream_consumer.py`

## Folder Layout
- `code/`
  - `spark/data_cleaning_spark.py` — Spark ETL from NDJSON/zst -> cleaned CSV (per-city + aggregated 2024 cut).
  - `batch/historical_predictions.py` — Batch RoBERTa inference over cleaned CSVs.
  - `streaming/producer.py` — Simulated live producer that streams cleaned rows to Kafka (`demo-stream`).
  - `streaming/consumer.py` — ML service consuming `demo-stream`, predicting emotions, emitting to `pred-stream`.
  - `dashboard/pred_stream_consumer.py` — Streamlit dashboard (live + historical explorer modes).
  - `notebooks/FINAL_BERT.ipynb`, `notebooks/FINAL_ROBERTA.ipynb` — model experimentation.
- `data/`
  - `city_submissions/` — Raw NDJSON per city (see “Data sources”).
  - `sample/clean.csv` — Cleaned sample for quick runs.
  - `sample/Roberta_Prediction.csv` — Sample batch predictions.

## Data Sources
- Raw Reddit submissions from the Pushshift top-40k subreddit dump ([link](https://www.reddit.com/r/pushshift/comments/1itme1k/separate_dump_files_for_the_top_40k_subreddits/)). Cities covered: Austin, Boston, Chicago, Dallas, Houston, Los Angeles, Miami, NYC, Orlando, Philadelphia, Phoenix, San Diego, San Francisco, Seattle, Washington DC.
- The pull was ~45 GB of NDJSON, filtered/cleaned down to 5M+ rows via Spark.
- Per-city NDJSON drops are **not** stored in git; fetch them from the shared drive listed in `data/city_submissions/README.md` and drop files as `<city>_submissions.zst`.
- Samples: `data/sample/clean.csv` (cleaned) and `data/sample/Roberta_Prediction.csv` (predictions) for quick tests.

## Environment & Install
- Python 3.10+ recommended.
- From repo root: `pip install -r requirements.txt`
- Environment variables (set via `.env` if desired):
  ```
  KAFKA_BOOTSTRAP=3.236.215.110:9092
  KAFKA_TOPIC=demo-stream
  KAFKA_PRED_TOPIC=pred-stream
  KAFKA_GROUP_ID=emotion-consumer
  KAFKA_PRED_GROUP_ID=pred-stream-reader
  MODEL_DIR=/path/to/roberta-goemotions-6class-final
  PRODUCER_CSV=/absolute/path/to/clean.csv               # defaults to term-project/data/sample/clean.csv
  PREDICTION_CSV_PATH=/absolute/path/to/Roberta_Prediction.csv
  HIST_INPUT_CSV=...                                     # for batch runs
  HIST_OUTPUT_CSV=...
  ```
- Spark 3.x and AWS credentials are required for historical ETL (reads/writes `s3://bigdatafinalprojectcs777/...`). Kafka 3.x (Java 11+) required for streaming.

## Historical Pipeline (Spark)
1) Decompress and clean NDJSON -> per-city CSVs -> aggregate 2024 cut:
   ```
   spark-submit term-project/code/spark/data_cleaning_spark.py
   ```
   - Downloads zstd archives from `s3://bigdatafinalprojectcs777/data/historical_data/compressed/`
   - Cleans text (`selftext`/`title`), lowercases, strips URLs/punct/stopwords, builds `text = title + content`
   - Writes per-city CSVs and a 2024-only aggregate to `data/historical_data/clean/` in S3
   - Output schema: `subreddit, created (ts), title, content, text`

## Batch Emotion Inference (Historical)
Run RoBERTa/GoEmotions over cleaned CSVs:
```
python term-project/code/batch/historical_predictions.py \
  --input term-project/data/sample/clean.csv \
  --output term-project/data/sample/Roberta_Prediction.csv \
  --model-dir /path/to/roberta-goemotions-6class-final \
  --batch-size 16
```
Adds `pred_label`, `pred_emotion`, `pred_confidence` to the cleaned columns above.

## Streaming Pipeline (Kafka)
- **Producer** (simulated live):
  ```
  python term-project/code/streaming/producer.py
  ```
- **Consumer / ML service** (classify + publish):
  ```
  python term-project/code/streaming/consumer.py
  ```
Topics: `demo-stream` (clean posts) -> `pred-stream` (enriched predictions). Keeps label parity with batch outputs.

## Dashboard
Streamlit app with live mode (Kafka) and explorer mode (CSV):
```
streamlit run term-project/code/dashboard/pred_stream_consumer.py
```
- Live: consumes `pred-stream`, shows emotion bars/wordcloud/cards.
- Explorer: reads `Roberta_Prediction.csv` with filters for subreddit, emotion, confidence, date, text search.

## Quick Visuals
- Live dashboard: ![Live dashboard](../images/dashboard/dashboard_live_1.png)
- Historical explorer: ![Historical dashboard](../images/dashboard/dashboard_historic_1.png)
- Batch output: ![Batch output](../images/batch_output/batch_output.jpeg)
