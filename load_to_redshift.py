import os
import psycopg2
import logging
from dotenv import load_dotenv
import boto3
from utils import read_manifest, update_manifest 


STREAMS_MANIFEST_KEY = "processing-metadata/loaded_stream_files.txt"
bucket = "music-etl-processed-data"

# === Load environment variables from .env (LOCAL ONLY) ===
load_dotenv()

# === Setup Logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# === Redshift connection ===
def get_redshift_connection():
    return psycopg2.connect(
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT", "5439")
    )

# === Create all main and staging tables ===
def create_tables(conn):
    queries = [
        # Raw
        """
        CREATE TABLE IF NOT EXISTS raw_users (
            user_id VARCHAR,
            user_name VARCHAR,
            user_age INT,
            user_country VARCHAR,
            created_at DATE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS raw_songs (
            id INT,
            track_id VARCHAR,
            artists VARCHAR(MAX),
            album_name VARCHAR,
            track_name VARCHAR(MAX),
            popularity INT,
            duration_ms INT,
            explicit SMALLINT,
            danceability FLOAT4,
            energy FLOAT4,
            key INT,
            loudness FLOAT4,
            mode INT,
            speechiness FLOAT4,
            acousticness FLOAT4,
            instrumentalness FLOAT4,
            liveness FLOAT4,
            valence FLOAT4,
            tempo FLOAT4,
            time_signature INT,
            track_genre VARCHAR
        );
        """,
        # Transformed
        """
        CREATE TABLE IF NOT EXISTS processed_users (
            user_id VARCHAR,
            user_name VARCHAR,
            user_age INT,
            user_country VARCHAR,
            created_at DATE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS processed_songs (
            id INT,
            track_id VARCHAR,
            artists VARCHAR(MAX),
            album_name VARCHAR,
            track_name VARCHAR(MAX),
            popularity INT,
            duration_ms INT,
            explicit SMALLINT,
            danceability FLOAT4,
            energy FLOAT4,
            key INT,
            loudness FLOAT4,
            mode INT,
            speechiness FLOAT4,
            acousticness FLOAT4,
            instrumentalness FLOAT4,
            liveness FLOAT4,
            valence FLOAT4,
            tempo FLOAT4,
            time_signature INT,
            track_genre VARCHAR,
            duration_sec FLOAT4
        );
        """,
        # Streams
        """
        CREATE TABLE IF NOT EXISTS transformed_streams (
            user_id VARCHAR,
            track_id VARCHAR,
            listen_time TIMESTAMP,
            hour INT
        );
        """,
        # Staging
        """CREATE TEMP TABLE IF NOT EXISTS staging_raw_users (LIKE raw_users);""",
        """CREATE TEMP TABLE IF NOT EXISTS staging_raw_songs (LIKE raw_songs);""",
        """CREATE TEMP TABLE IF NOT EXISTS staging_processed_users (LIKE processed_users);""",
        """CREATE TEMP TABLE IF NOT EXISTS staging_processed_songs (LIKE processed_songs);"""
    ]
    with conn.cursor() as cur:
        for query in queries:
            logger.info(f"🛠 Executing: {query.split('(')[0].strip()}")
            cur.execute(query)
        conn.commit()

# === COPY into staging tables ===
def copy_to_staging(conn, table, s3_key):
    bucket = "music-etl-processed-data"
    role_arn = os.getenv("REDSHIFT_IAM_ROLE")
    staging_table = f"staging_{table}"  # e.g. staging_raw_users

    copy_sql = f"""
    COPY {staging_table}
    FROM 's3://{bucket}/{s3_key}'
    IAM_ROLE '{role_arn}'
    FORMAT AS PARQUET;
    """

    with conn.cursor() as cur:
        logger.info(f"COPY into {staging_table} from {s3_key}")
        cur.execute(copy_sql)
        conn.commit()

# === Deduplicated INSERT from staging to target ===
def insert_dedup(conn, table, key_column):
    staging_table = f"staging_{table}"
    insert_sql = f"""
    INSERT INTO {table}
    SELECT *
    FROM {staging_table} s
    WHERE NOT EXISTS (
        SELECT 1 FROM {table} r WHERE r.{key_column} = s.{key_column}
    );
    """
    with conn.cursor() as cur:
        logger.info(f"Deduplicating insert from {staging_table} to {table}")
        cur.execute(insert_sql)
        conn.commit()

# === Drop staging tables ===
def drop_staging(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS staging_raw_users;")
        cur.execute("DROP TABLE IF EXISTS staging_raw_songs;")
        cur.execute("DROP TABLE IF EXISTS staging_processed_users;")
        cur.execute("DROP TABLE IF EXISTS staging_processed_songs;")
        conn.commit()
        logger.info("Dropped all staging tables.")

# === Get all transformed stream files ===
def get_transformed_stream_keys(bucket, prefix="transformed-data/"):
    s3 = boto3.client("s3")
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("_transformed.parquet") and "streams" in key:
                keys.append(key)
    return keys

# === COPY transformed streams directly ===
def copy_from_s3(conn, table, s3_key):
    bucket = "music-etl-processed-data"
    role_arn = os.getenv("REDSHIFT_IAM_ROLE")

    copy_sql = f"""
    COPY {table}
    FROM 's3://{bucket}/{s3_key}'
    IAM_ROLE '{role_arn}'
    FORMAT AS PARQUET;
    """

    with conn.cursor() as cur:
        logger.info(f"Loading s3://{bucket}/{s3_key} into table `{table}`")
        cur.execute(copy_sql)
        conn.commit()
        logger.info(f"Loaded {s3_key} into {table}")

# === Load all data ===
def load_all():
    try:
        logger.info("Connecting to Redshift...")
        conn = get_redshift_connection()

        logger.info("Creating all tables...")
        create_tables(conn)

        # RAW USERS
        copy_to_staging(conn, "raw_users", "validated-data/validated_users.parquet")
        insert_dedup(conn, "raw_users", "user_id")

        # RAW SONGS
        copy_to_staging(conn, "raw_songs", "validated-data/validated_songs.parquet")
        insert_dedup(conn, "raw_songs", "track_id")

        # PROCESSED USERS
        copy_to_staging(conn, "processed_users", "transformed-data/transformed_users.parquet")
        insert_dedup(conn, "processed_users", "user_id")

        # PROCESSED SONGS
        copy_to_staging(conn, "processed_songs", "transformed-data/transformed_songs.parquet")
        insert_dedup(conn, "processed_songs", "track_id")

        drop_staging(conn)

        # STREAMS
        all_stream_keys = get_transformed_stream_keys(bucket)
        already_loaded = read_manifest(bucket, STREAMS_MANIFEST_KEY)

        new_keys = [k for k in all_stream_keys if k not in already_loaded]

        if not new_keys:
            logger.info("No new transformed stream files to load.")
        else:
            for key in new_keys:
                try:
                    copy_from_s3(conn, "transformed_streams", key)
                except Exception:
                    logger.exception(f"Failed to load {key}. Skipping.")
                    continue

            update_manifest(bucket, STREAMS_MANIFEST_KEY, new_keys)
            logger.info(f"Loaded {len(new_keys)} new stream file(s) into Redshift.")
        

    except Exception as e:
        logger.exception("Redshift load failed.")
        raise

if __name__ == "__main__":
    load_all()