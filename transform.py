import pandas as pd
import os
import logging
from utils import (
    read_parquet_from_s3,
    write_parquet_to_s3,
    list_s3_files,
    read_manifest,
    update_manifest
)

# === Setup Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BUCKET = "music-etl-processed-data"

# Keys
USERS_KEY = "validated-data/validated_users.parquet"
SONGS_KEY = "validated-data/validated_songs.parquet"
STREAMS_PREFIX = "validated-data/"
TRANSFORMED_PREFIX = "transformed-data/"
MANIFEST_KEY = "processing-metadata/transformed_stream_files.txt"

def transform_users():
    try:
        df = read_parquet_from_s3(BUCKET, USERS_KEY)
        df["user_id"] = df["user_id"].astype(str)
        df["user_country"] = df["user_country"].str.title().str.strip()
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

        write_parquet_to_s3(df, BUCKET, f"{TRANSFORMED_PREFIX}transformed_users.parquet")
        logger.info(f"Users transformed and saved: {len(df)} records.")
    except Exception:
        logger.exception("Failed transforming users.")

def transform_songs():
    try:
        df = read_parquet_from_s3(BUCKET, SONGS_KEY)
        df["track_id"] = df["track_id"].astype(str)

        # Clean genres
        df["track_genre"] = df["track_genre"].str.lower().str.strip()

        # Convert and add derived columns
        df["duration_ms"] = pd.to_numeric(df["duration_ms"], errors="coerce")
        df["duration_sec"] = (df["duration_ms"] / 1000).round(2)
        df["duration_sec"] = df["duration_sec"].astype("float32")

        # Coerce numeric/boolean-like fields
        for col in ["popularity", "explicit", "danceability", "energy", "key", "loudness",
                    "mode", "speechiness", "acousticness", "instrumentalness",
                    "liveness", "valence", "tempo", "time_signature"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        write_parquet_to_s3(df, BUCKET, f"{TRANSFORMED_PREFIX}transformed_songs.parquet")
        logger.info(f"Songs transformed and saved: {len(df)} records.")
    except Exception:
        logger.exception("Failed transforming songs.")

def transform_stream_batches():
    try:
        all_keys = list_s3_files(BUCKET, STREAMS_PREFIX)
        processed = read_manifest(BUCKET, MANIFEST_KEY)

        new_files = [
            k for k in all_keys
            if k.endswith("_validated.parquet") and "streams" in k and k not in processed
        ]

        if not new_files:
            logger.info("No new validated stream files to transform.")
            return

        for key in new_files:
            try:
                logger.info(f"Transforming stream batch: {key}")
                df = read_parquet_from_s3(BUCKET, key)

                df["user_id"] = df["user_id"].astype(str)
                df["track_id"] = df["track_id"].astype(str)
                df["listen_time"] = pd.to_datetime(df["listen_time"], errors="coerce")
                df["hour"] = df["listen_time"].dt.hour

                base = os.path.basename(key).replace("_validated.parquet", "_transformed.parquet")
                output_key = f"{TRANSFORMED_PREFIX}{base}"
                write_parquet_to_s3(df, BUCKET, output_key)

                update_manifest(BUCKET, MANIFEST_KEY, [key])
                logger.info(f"Finished transforming: {key} → {len(df)} records")

            except Exception:
                logger.exception(f"Failed transforming stream file: {key}. Skipping.")

    except Exception:
        logger.exception("Failed during stream batch transformation.")

def transform():
    logger.info("Starting transformation phase (users, songs, new streams)...")
    transform_users()
    transform_songs()
    transform_stream_batches()
    logger.info("Transformation complete.")

if __name__ == "__main__":
    transform()
