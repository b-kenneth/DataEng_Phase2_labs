import pandas as pd
import os
import logging
from utils import (
    read_csv_from_s3,
    write_csv_to_s3,
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
USERS_KEY = "validated-data/validated_users.csv"
SONGS_KEY = "validated-data/validated_songs.csv"
STREAMS_PREFIX = "validated-data/"
TRANSFORMED_PREFIX = "transformed-data/"
MANIFEST_KEY = "processing-metadata/transformed_stream_files.txt"

def transform_users():
    try:
        df = read_csv_from_s3(BUCKET, USERS_KEY)
        df["user_id"] = df["user_id"].astype(str)
        write_csv_to_s3(df, BUCKET, f"{TRANSFORMED_PREFIX}transformed_users.csv")
        logger.info("✅ Users transformed and saved.")
    except Exception:
        logger.exception("❌ Failed transforming users.")

def transform_songs():
    try:
        df = read_csv_from_s3(BUCKET, SONGS_KEY)
        df["track_id"] = df["track_id"].astype(str)
        df["track_genre"] = df["track_genre"].str.lower().str.strip()
        write_csv_to_s3(df, BUCKET, f"{TRANSFORMED_PREFIX}transformed_songs.csv")
        logger.info("✅ Songs transformed and saved.")
    except Exception:
        logger.exception("❌ Failed transforming songs.")

def transform_stream_batches():
    try:
        all_keys = list_s3_files(BUCKET, STREAMS_PREFIX)
        processed = read_manifest(BUCKET, MANIFEST_KEY)

        new_files = [
            k for k in all_keys
            if k.endswith("_validated.csv") and "streams" in k and k not in processed
        ]

        if not new_files:
            logger.info("📭 No new validated stream files to transform.")
            return

        for key in new_files:
            try:
                logger.info(f"📥 Transforming stream batch: {key}")
                df = read_csv_from_s3(BUCKET, key)

                df["user_id"] = df["user_id"].astype(str)
                df["track_id"] = df["track_id"].astype(str)
                df["listen_time"] = pd.to_datetime(df["listen_time"], errors="coerce")
                df["hour"] = df["listen_time"].dt.hour

                base = os.path.basename(key).replace("_validated.csv", "_transformed.csv")
                output_key = f"{TRANSFORMED_PREFIX}{base}"
                write_csv_to_s3(df, BUCKET, output_key)

                update_manifest(BUCKET, MANIFEST_KEY, [key])
                logger.info(f"✅ Finished transforming stream file: {key}")

            except Exception:
                logger.exception(f"❌ Failed to transform stream file: {key}. Skipping.")

    except Exception:
        logger.exception("❌ Failed during streaming batch transformation.")

def transform():
    logger.info("🚀 Starting transformation phase (users, songs, new streams)...")
    transform_users()
    transform_songs()
    transform_stream_batches()
    logger.info("🏁 Transformation complete.")

if __name__ == "__main__":
    transform()



# import pandas as pd
# import logging
# from utils import read_csv_from_s3, write_csv_to_s3

# # === Setup Logging ===
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )
# logger = logging.getLogger(__name__)

# BUCKET = "music-etl-processed-data"

# # S3 Keys
# USERS_KEY = "validated-data/validated_users.csv"
# SONGS_KEY = "validated-data/validated_songs.csv"
# STREAMS_KEY = "validated-data/validated_streams.csv"
# OUTPUT_KEY = "transformed-data/transformed_streams.csv"

# def transform():
#     try:
#         logger.info("Starting data transformation process...")

#         # === Load validated data ===
#         users_df = read_csv_from_s3(BUCKET, USERS_KEY)
#         songs_df = read_csv_from_s3(BUCKET, SONGS_KEY)
#         streams_df = read_csv_from_s3(BUCKET, STREAMS_KEY)
#         logger.info("All validated datasets loaded.")

#         # === Normalize + prepare columns ===
#         songs_df["track_id"] = songs_df["track_id"].astype(str)
#         users_df["user_id"] = users_df["user_id"].astype(str)
#         streams_df["track_id"] = streams_df["track_id"].astype(str)
#         streams_df["user_id"] = streams_df["user_id"].astype(str)

#         logger.info("Normalized ID columns for joins.")

#         # === Join ===
#         merged_df = pd.merge(streams_df, songs_df, on="track_id", how="left")
#         merged_df = pd.merge(merged_df, users_df, on="user_id", how="left")

#         logger.info(f"Completed joins. Merged shape: {merged_df.shape}")

#         # Drop rows missing critical metadata
#         before_drop = len(merged_df)
#         merged_df.dropna(subset=["track_name", "user_name", "track_genre"], inplace=True)
#         after_drop = len(merged_df)
#         dropped = before_drop - after_drop
#         if dropped > 0:
#             logger.warning(f"Dropped {dropped} rows with missing metadata.")

#         # === Add derived columns ===
#         merged_df["duration_sec"] = merged_df["duration_ms"] / 1000
#         merged_df["hour"] = pd.to_datetime(merged_df["listen_time"]).dt.hour
#         merged_df["track_genre"] = merged_df["track_genre"].str.lower().str.strip()

#         logger.info("Added derived columns: duration_sec, hour, normalized genre.")

#         # === Save ===
#         write_csv_to_s3(merged_df, BUCKET, OUTPUT_KEY)
#         logger.info(f"Transformation complete. Data saved to s3://{BUCKET}/{OUTPUT_KEY}")

#     except Exception as e:
#         logger.exception("Failed during transformation.")
#         raise

# if __name__ == "__main__":
#     transform()