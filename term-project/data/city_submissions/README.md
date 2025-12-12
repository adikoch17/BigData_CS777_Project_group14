# City Submissions (Raw Reddit NDJSON)

Raw Pushshift/Reddit submission dumps by city live outside git to keep the repo lean. Place the compressed NDJSON files here before running Spark cleaning.

## What’s in this folder
- Per-city NDJSON dumps compressed as `.zst` (newline-delimited JSON).
- Source: Pushshift top-40k subreddit dump ([context](https://www.reddit.com/r/pushshift/comments/1itme1k/separate_dump_files_for_the_top_40k_subreddits/)).
- Download location: shared drive https://drive.google.com/drive/folders/1UtVvo-GQk6UNNCAcPrW7WvVnamwisJ98?usp=sharing
- Total raw size pulled: ~45 GB; downstream Spark cleaning filters that to 5M+ rows.

## Cities covered
Austin, Boston, Chicago, Dallas, Houston, LosAngeles, Miami, NYC, Orlando, Philadelphia, Phoenix, San Diego, San Francisco, Seattle, WashingtonDC.

## Expected filenames (drop into this folder)
```
Austin_submissions.zst
Boston_submissions.zst
Chicago_submissions.zst
Dallas_submissions.zst
houston_submissions.zst
LosAngeles_submissions.zst
Miami_submissions.zst
nyc_submissions.zst
orlando_submissions.zst
philadelphia_submissions.zst
phoenix_submissions.zst
sandiego_submissions.zst
sanfrancisco_submissions.zst
Seattle_submissions.zst
washingtondc_submissions.zst
```
Name/letter case matches what `term-project/code/spark/data_cleaning_spark.py` expects.

## Record format (per line)
- Newline-delimited JSON objects with standard Pushshift submission fields, e.g.: `id`, `subreddit`, `created_utc`, `title`, `selftext`, `author`, `score`, `num_comments`, `permalink`, `url`, `over_18`, `stickied`, `locked`, `distinguished`, etc. Field presence can vary by dump.

## How to stage the data
1) Download the `.zst` files from the drive above into `term-project/data/city_submissions/`.
2) (Optional) Spot-check contents without full decompress:
   ```
   zstd -d -c Austin_submissions.zst | head -n 3
   ```
3) Run cleaning:
   ```
   spark-submit term-project/code/spark/data_cleaning_spark.py
   ```
   This decompresses, cleans, and emits per-city CSVs plus a 2024 aggregate to S3.

## Notes
- Keep raw dumps out of version control; they are large and user-generated.
- Add new cities by matching the `<city>_submissions.zst` naming and updating the `object_list` in `spark/data_cleaning_spark.py` if needed.
