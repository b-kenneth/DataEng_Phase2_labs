import pandas as pd
import os
import logging
from utils import (
    read_csv_from_s3,
    list_s3_files,
    read_manifest,
    update_manifest,
    write_parquet_to_s3
)

# === Setup Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# === S3 Buckets and Keys ===
BUCKET_IN = "music-streaming-etl-data"
BUCKET_OUT = "music-etl-processed-data"

SONGS_KEY = "metadata/songs/songs.csv"
USERS_KEY = "metadata/users/users.csv"

SONGS_OUT = "validated-data/validated_songs.parquet"
USERS_OUT = "validated-data/validated_users.parquet"

STREAMS_PREFIX = "streams/"
STREAMS_VALIDATED_PREFIX = "validated-data/"
MANIFEST_KEY = "processing-metadata/processed_stream_files.txt"

def validate_columns(df, required_cols, dataset_name):
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        msg = f"{dataset_name} missing required columns: {missing}"
        logger.error(msg)
        raise ValueError(msg)
    logger.info(f"{dataset_name} passed column validation.")
    return True

def validate_users(df):
    df.dropna(subset=["user_id", "user_name", "user_age", "created_at"], inplace=True)
    df["user_id"] = df["user_id"].astype(str)

    # Ensure age is numeric and within sane bounds
    df = df[pd.to_numeric(df["user_age"], errors="coerce").between(13, 120)]

    # Parse and validate created_at
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["user_age"] = pd.to_numeric(df["user_age"], errors="coerce").fillna(0).astype("int32")

    df.dropna(subset=["created_at"], inplace=True)

    logger.info(f"✅ Users validation complete: {len(df)} records")
    return df

def validate_songs(df):
    # Drop rows with missing critical fields
    df.dropna(subset=["track_id", "track_name", "popularity", "duration_ms", "track_genre"], inplace=True)
    df["track_id"] = df["track_id"].astype(str)

    # Ensure popularity and duration_ms are numeric
    df = df[pd.to_numeric(df["popularity"], errors="coerce").notna()]
    df = df[pd.to_numeric(df["duration_ms"], errors="coerce").notna()]

    # Cast types to Redshift-compatible formats
    df["id"] = df["id"].astype("int32")
    df["popularity"] = df["popularity"].astype("int32")
    df["duration_ms"] = df["duration_ms"].astype("int32")
    df["explicit"] = df["explicit"].astype("int8")  # 0 or 1, use int8
    df["danceability"] = df["danceability"].astype("float32")
    df["energy"] = df["energy"].astype("float32")
    df["key"] = pd.to_numeric(df["key"], errors="coerce").fillna(0).astype("int32")
    df["loudness"] = df["loudness"].astype("float32")
    df["mode"] = pd.to_numeric(df["mode"], errors="coerce").fillna(0).astype("int32")
    df["speechiness"] = df["speechiness"].astype("float32")
    df["acousticness"] = df["acousticness"].astype("float32")
    df["instrumentalness"] = df["instrumentalness"].astype("float32")
    df["liveness"] = df["liveness"].astype("float32")
    df["valence"] = df["valence"].astype("float32")
    df["tempo"] = df["tempo"].astype("float32")
    df["time_signature"] = pd.to_numeric(df["time_signature"], errors="coerce").fillna(0).astype("int32")

    # Ensure strings are properly formatted
    df["track_genre"] = df["track_genre"].str.lower().str.strip()
    df["track_name"] = df["track_name"].astype(str)
    df["album_name"] = df["album_name"].astype(str)
    df["artists"] = df["artists"].astype(str)

    logger.info(f"✅ Songs validation complete: {len(df)} records")
    return df


def validate_streams(df, stream_key):
    df.dropna(subset=["user_id", "track_id", "listen_time"], inplace=True)
    df["user_id"] = df["user_id"].astype(str)
    df["track_id"] = df["track_id"].astype(str)
    df["listen_time"] = pd.to_datetime(df["listen_time"], errors="coerce")
    dropped = df["listen_time"].isna().sum()
    if dropped > 0:
        logger.warning(f"🕒 Dropping {dropped} rows with invalid timestamps.")
        df.dropna(subset=["listen_time"], inplace=True)
    logger.info(f"✅ Streams validation complete: {stream_key} → {len(df)} records")
    return df

def extract_and_validate():
    try:
        logger.info("🚀 Starting extraction and validation process...")

        # === Load metadata ===
        songs_df = read_csv_from_s3(BUCKET_IN, SONGS_KEY)
        users_df = read_csv_from_s3(BUCKET_IN, USERS_KEY)
        logger.info("✅ Metadata files loaded.")

        # === Validate columns ===
        validate_columns(songs_df, [
            "track_id", "track_name", "artists", "popularity", "duration_ms", "track_genre"
        ], "songs.csv")
        validate_columns(users_df, [
            "user_id", "user_name", "user_age", "user_country", "created_at"
        ], "users.csv")

        # === Validate content ===
        songs_df = validate_songs(songs_df)
        users_df = validate_users(users_df)

        # === Save validated metadata ===
        write_parquet_to_s3(songs_df, BUCKET_OUT, SONGS_OUT)
        write_parquet_to_s3(users_df, BUCKET_OUT, USERS_OUT)
        logger.info("✅ Validated metadata saved.")

        # === Streaming data: detect new files ===
        logger.info("🔎 Scanning for new streaming files...")
        all_stream_keys = list_s3_files(BUCKET_IN, STREAMS_PREFIX)
        processed_keys = read_manifest(BUCKET_OUT, MANIFEST_KEY)

        new_files = [
            key for key in all_stream_keys
            if key.endswith(".csv") and key not in processed_keys
        ]
        if not new_files:
            logger.info("📭 No new streaming files to process.")
            return

        for stream_key in new_files:
            try:
                logger.info(f"📥 Processing stream file: {stream_key}")
                df = read_csv_from_s3(BUCKET_IN, stream_key)

                # === Validate schema and content ===
                validate_columns(df, ["user_id", "track_id", "listen_time"], stream_key)
                df = validate_streams(df, stream_key)

                # Save validated stream data
                stream_name = os.path.basename(stream_key).replace(".csv", "")
                validated_key = f"{STREAMS_VALIDATED_PREFIX}{stream_name}_validated.parquet"
                write_parquet_to_s3(df, BUCKET_OUT, validated_key)

                # Update manifest
                update_manifest(BUCKET_OUT, MANIFEST_KEY, [stream_key])
                logger.info(f"✅ Finished processing: {stream_key}")

            except Exception as stream_err:
                logger.exception(f"❌ Failed processing {stream_key}. Skipping.")

        logger.info("🏁 Streaming ingestion complete.")

    except Exception as e:
        logger.exception("❌ Failed during extract_and_validate execution.")
        raise

if __name__ == "__main__":
    extract_and_validate()




# import pandas as pd
# import os
# import logging
# from utils import (
#     read_csv_from_s3,
#     write_csv_to_s3,
#     list_s3_files,
#     read_manifest,
#     update_manifest
# )

# # === Setup Logging ===
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )
# logger = logging.getLogger(__name__)

# # === S3 Buckets and Keys ===
# BUCKET_IN = "music-streaming-etl-data"
# BUCKET_OUT = "music-etl-processed-data"

# SONGS_KEY = "metadata/songs/songs.csv"
# USERS_KEY = "metadata/users/users.csv"

# SONGS_OUT = "validated-data/validated_songs.csv"
# USERS_OUT = "validated-data/validated_users.csv"

# STREAMS_PREFIX = "streams/"
# STREAMS_VALIDATED_PREFIX = "validated-data/"
# MANIFEST_KEY = "processing-metadata/processed_stream_files.txt"

# def validate_columns(df, required_cols, dataset_name):
#     missing = [col for col in required_cols if col not in df.columns]
#     if missing:
#         msg = f"{dataset_name} missing required columns: {missing}"
#         logger.error(msg)
#         raise ValueError(msg)
#     logger.info(f"{dataset_name} passed column validation.")
#     return True

# def extract_and_validate():
#     try:
#         logger.info("🚀 Starting extraction and validation process...")

#         # === Load metadata ===
#         songs_df = read_csv_from_s3(BUCKET_IN, SONGS_KEY)
#         users_df = read_csv_from_s3(BUCKET_IN, USERS_KEY)
#         logger.info("✅ Metadata files loaded.")

#         # === Validate metadata ===
#         validate_columns(songs_df, [
#             "track_id", "track_name", "artists", "popularity", "duration_ms", "track_genre"
#         ], "songs.csv")
#         validate_columns(users_df, [
#             "user_id", "user_name", "user_age", "user_country", "created_at"
#         ], "users.csv")

#         # === Clean metadata ===
#         songs_df.dropna(subset=["track_id", "track_name"], inplace=True)
#         users_df.dropna(subset=["user_id", "user_name"], inplace=True)
#         songs_df["track_id"] = songs_df["track_id"].astype(str)
#         users_df["user_id"] = users_df["user_id"].astype(str)

#         # === Save validated metadata ===
#         write_csv_to_s3(songs_df, BUCKET_OUT, SONGS_OUT)
#         write_csv_to_s3(users_df, BUCKET_OUT, USERS_OUT)
#         logger.info("✅ Validated metadata saved.")

#         # === Streaming data: detect new files ===
#         logger.info("🔎 Scanning for new streaming files...")
#         all_stream_keys = list_s3_files(BUCKET_IN, STREAMS_PREFIX)
#         processed_keys = read_manifest(BUCKET_OUT, MANIFEST_KEY)

#         # new_files = [key for key in all_stream_keys if key not in processed_keys]
#         new_files = [
#             key for key in all_stream_keys
#             if key.endswith(".csv") and key not in processed_keys
#         ]
#         if not new_files:
#             logger.info("📭 No new streaming files to process.")
#             return

#         for stream_key in new_files:
#             try:
#                 logger.info(f"📥 Processing stream file: {stream_key}")
#                 df = read_csv_from_s3(BUCKET_IN, stream_key)

#                 # === Validate streaming schema ===
#                 validate_columns(df, ["user_id", "track_id", "listen_time"], stream_key)

#                 df.dropna(subset=["user_id", "track_id", "listen_time"], inplace=True)
#                 df["user_id"] = df["user_id"].astype(str)
#                 df["track_id"] = df["track_id"].astype(str)
#                 df["listen_time"] = pd.to_datetime(df["listen_time"], errors="coerce")

#                 dropped = df["listen_time"].isna().sum()
#                 if dropped > 0:
#                     logger.warning(f"🕒 Dropping {dropped} rows with invalid timestamps.")
#                     df.dropna(subset=["listen_time"], inplace=True)

#                 # Save validated stream data
#                 stream_name = os.path.basename(stream_key).replace(".csv", "")
#                 validated_key = f"{STREAMS_VALIDATED_PREFIX}{stream_name}_validated.csv"
#                 write_csv_to_s3(df, BUCKET_OUT, validated_key)

#                 # Update manifest
#                 update_manifest(BUCKET_OUT, MANIFEST_KEY, [stream_key])
#                 logger.info(f"✅ Finished processing: {stream_key}")

#             except Exception as stream_err:
#                 logger.exception(f"❌ Failed processing {stream_key}. Skipping.")

#         logger.info("🏁 Streaming ingestion complete.")

#     except Exception as e:
#         logger.exception("❌ Failed during extract_and_validate execution.")
#         raise

# if __name__ == "__main__":
#     extract_and_validate()