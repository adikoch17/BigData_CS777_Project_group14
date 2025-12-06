import boto3
import zstandard as zstd
import os
import shutil
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, lit, lower, regexp_replace, 
    split, trim, coalesce, concat_ws
)
from pyspark.sql.types import StringType
from pyspark.ml.feature import StopWordsRemover

# ==========================================
# 1. Configuration & Setup
# ==========================================
BUCKET = "bigdatafinalprojectcs777"
INPUT_KEY = "data/historical_data/compressed/"
OUTPUT_KEY = "data/historical_data/clean/"

# Initialize S3 Client
s3 = boto3.client("s3")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("RedditDataCleaning") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Suppress excessive logging
spark.sparkContext.setLogLevel("ERROR")

# List of files to process
object_list = [
    'Miami_submissions.zst',
    'orlando_submissions.zst',
    'phoenix_submissions.zst',
    'Dallas_submissions.zst',
    'washingtondc_submissions.zst',
    'sandiego_submissions.zst',
    'philadelphia_submissions.zst',
    'sanfrancisco_submissions.zst',
    'houston_submissions.zst',
    'boston_submissions.zst',
    'chicago_submissions.zst',
    'nyc_submissions.zst',
    'Seattle_submissions.zst',
    'Austin_submissions.zst',
    'LosAngeles_submissions.zst'
]

# ==========================================
# 2. Decompression (Local Processing)
# ==========================================
dctx = zstd.ZstdDecompressor()
local_json_files = []

print("Starting decompression...")
for o in object_list:
    in_key = INPUT_KEY + o
    local_path = f"/tmp/{o}_raw_1.jsonl"
    local_json_files.append(local_path)
    
    # Check if file already exists to save time, else download and decompress
    if not os.path.exists(local_path):
        print(f"Downloading and decompressing {o}...")
        response = s3.get_object(Bucket=BUCKET, Key=in_key)
        with dctx.stream_reader(response["Body"]) as reader, open(local_path, "wb") as out:
            while True:
                chunk = reader.read(2**20)  # 1 MB chunks
                if not chunk:
                    break
                out.write(chunk)
        print(f"Finished {local_path}")
    else:
        print(f"File {local_path} already exists, skipping download.")

print("Full decompression list:", local_json_files)

# ==========================================
# 3. Define Spark Cleaning Function
# ==========================================
def clean_reddit_json_to_csv_spark(input_path, output_path):
    """
    Reads JSONL, applies NLP cleaning to content, creates 'text' column, 
    and saves as CSV.
    """
    # Read JSON file
    df = spark.read.json(input_path)
    
    # Ensure required columns exist
    for c in ["selftext", "title", "created_utc", "subreddit"]:
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast(StringType()))

    # Basic Transformation (Timestamp & Renaming)
    df_clean = df.withColumn("created", from_unixtime(col("created_utc")).cast("timestamp")) \
                 .withColumnRenamed("selftext", "content") \
                 .withColumn("content", coalesce(col("content"), lit(""))) \
                 .withColumn("title", coalesce(col("title"), lit("")))

    # --- NLP Preprocessing Steps on 'content' ---
    
    # A. Handling HTML Tags and URLs
    df_clean = df_clean.withColumn("content", regexp_replace(col("content"), r'<[^>]+>', ' ')) \
                       .withColumn("content", regexp_replace(col("content"), r'http\S+|www\S+', ' '))

    # B. Lowercasing
    df_clean = df_clean.withColumn("content", lower(col("content")))

    # C. Removing Numbers
    df_clean = df_clean.withColumn("content", regexp_replace(col("content"), r'\d+', ' '))

    # D. Removing Punctuation (keep only letters and spaces)
    df_clean = df_clean.withColumn("content", regexp_replace(col("content"), r'[^a-z\s]', ' '))

    # E. Tokenization (Split by whitespace) & Trim
    df_clean = df_clean.withColumn("content", regexp_replace(col("content"), r'\s+', ' '))
    df_clean = df_clean.withColumn("content", trim(col("content")))
    df_clean = df_clean.withColumn("content_tokens", split(col("content"), " "))

    # F. Removing Stop Words
    remover = StopWordsRemover(inputCol="content_tokens", outputCol="content_filtered")
    df_clean = remover.transform(df_clean)

    # G. Join tokens back to regular string
    df_clean = df_clean.withColumn("content_final", concat_ws(" ", col("content_filtered")))

    # --- Create 'text' Column (Title + Cleaned Content) ---
    df_clean = df_clean.withColumn("text", concat_ws(" ", col("title"), col("content_final")))

    # Select Final Columns
    final_output = df_clean.select("subreddit", "created", "title", "content_final", "text") \
                           .withColumnRenamed("content_final", "content") \
                           .dropna(subset=["title"])

    # Write to Single CSV
    temp_folder = output_path + "_temp_spark"
    
    # Coalesce(1) ensures single file output
    final_output.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .option("quoteAll", "true") \
        .option("escape", '"') \
        .csv(temp_folder)
    
    # Move the part-file to the specific output path
    part_file = glob.glob(f"{temp_folder}/part-*.csv")[0]
    shutil.move(part_file, output_path)
    shutil.rmtree(temp_folder)
    
    print(f"Cleaned CSV written to {output_path}")

# ==========================================
# 4. Process Individual Files & Upload
# ==========================================
output_list = []

print("Starting Spark Processing...")
for i in local_json_files:
    city = os.path.basename(i).split('_submissions')[0]
    output_path = f"/tmp/{city}_clean.csv"
    
    clean_reddit_json_to_csv_spark(i, output_path)
    output_list.append(output_path)
    
    # Upload individual city CSV
    s3.upload_file(output_path, BUCKET, OUTPUT_KEY + os.path.basename(output_path))
    print(f"Uploaded {output_path} to S3")

# ==========================================
# 5. Aggregate, Filter 2024, and Final Upload
# ==========================================
print("Aggregating and filtering for 2024...")

# Read all processed CSVs back into Spark
final_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiLine", "true") \
    .csv(output_list)

# Ensure 'created' is timestamp
final_df = final_df.withColumn("created", col("created").cast("timestamp"))

# Filter for data from 2024 onwards
data24 = final_df.filter(col("created") >= lit("2024-01-01"))

print(f"Total historical rows: {final_df.count()}")
print(f"2024 rows: {data24.count()}")

# Write 2024 data to local CSV
local_24_path = "/tmp/data24.csv"
temp_24_folder = "/tmp/data24_spark_temp"

data24.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .option("quoteAll", "true") \
    .option("escape", '"') \
    .csv(temp_24_folder)

# Rename and cleanup
part_file = glob.glob(f"{temp_24_folder}/part-*.csv")[0]
if os.path.exists(local_24_path):
    os.remove(local_24_path)
shutil.move(part_file, local_24_path)
shutil.rmtree(temp_24_folder)

# Final Upload
s3.upload_file(local_24_path, BUCKET, OUTPUT_KEY + "data24.csv")
print(f"Successfully uploaded {local_24_path} to S3: {OUTPUT_KEY}data24.csv")

# Stop Spark
spark.stop()