import os
import psycopg2
import logging
from dotenv import load_dotenv
import boto3

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

# === Create Redshift Tables ===
def create_tables(conn):
    queries = [
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
        """
        CREATE TABLE IF NOT EXISTS transformed_streams (
            user_id VARCHAR,
            track_id VARCHAR,
            listen_time TIMESTAMP,
            hour INT
        );
        """
    ]
    with conn.cursor() as cur:
        for query in queries:
            logger.info(f"🛠 Executing: {query.split('(')[0].strip()}")
            cur.execute(query)
        conn.commit()

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

# === COPY Parquet from S3 into Redshift ===
def copy_from_s3(conn, table, s3_key, region="us-east-1"):
    bucket = "music-etl-processed-data"
    role_arn = os.getenv("REDSHIFT_IAM_ROLE")

    copy_sql = f"""
    COPY {table}
    FROM 's3://{bucket}/{s3_key}'
    IAM_ROLE '{role_arn}'
    FORMAT AS PARQUET;
    """

    with conn.cursor() as cur:
        logger.info(f"🔄 Loading s3://{bucket}/{s3_key} into table `{table}` (Parquet)")
        cur.execute(copy_sql)
        conn.commit()
        logger.info(f"✅ Loaded {s3_key} into {table}")

# === Load everything into Redshift ===
def load_all():
    try:
        logger.info("🚀 Connecting to Redshift...")
        conn = get_redshift_connection()

        logger.info("📐 Creating tables if not present...")
        create_tables(conn)

        logger.info("📤 Loading raw users and songs...")
        copy_from_s3(conn, "raw_users", "validated-data/validated_users.parquet")
        copy_from_s3(conn, "raw_songs", "validated-data/validated_songs.parquet")

        logger.info("📤 Loading all transformed stream files...")
        stream_keys = get_transformed_stream_keys("music-etl-processed-data")
        if not stream_keys:
            logger.info("📭 No transformed stream files to load.")
        for key in stream_keys:
            copy_from_s3(conn, "transformed_streams", key)

        conn.close()
        logger.info("🏁 Redshift loading complete.")

    except Exception as e:
        logger.exception("❌ Redshift load failed.")
        raise

if __name__ == "__main__":
    load_all()



# import os
# import psycopg2
# import logging
# from dotenv import load_dotenv
# import boto3

# # === Load environment variables from .env (LOCAL ONLY) ===
# load_dotenv()

# # === Setup Logging ===
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# logger = logging.getLogger(__name__)

# # === Redshift connection ===
# def get_redshift_connection():
#     return psycopg2.connect(
#         dbname=os.getenv("REDSHIFT_DB"),
#         user=os.getenv("REDSHIFT_USER"),
#         password=os.getenv("REDSHIFT_PASSWORD"),
#         host=os.getenv("REDSHIFT_HOST"),
#         port=os.getenv("REDSHIFT_PORT", "5439")
#     )

# # === Create tables ===
# def create_tables(conn):
#     queries = [
#         """
#         CREATE TABLE IF NOT EXISTS raw_users (
#             user_id VARCHAR,
#             user_name VARCHAR,
#             user_age INT,
#             user_country VARCHAR,
#             created_at DATE
#         );
#         """,
#         """
#         CREATE TABLE IF NOT EXISTS raw_songs (
#             id VARCHAR,
#             track_id VARCHAR,
#             artists VARCHAR(MAX),
#             album_name VARCHAR,
#             track_name VARCHAR(MAX),
#             popularity VARCHAR,
#             duration_ms VARCHAR,
#             explicit VARCHAR,
#             danceability VARCHAR,
#             energy VARCHAR,
#             key VARCHAR,
#             loudness VARCHAR,
#             mode VARCHAR,
#             speechiness VARCHAR,
#             acousticness VARCHAR,
#             instrumentalness VARCHAR,
#             liveness VARCHAR,
#             valence VARCHAR,
#             tempo VARCHAR,
#             time_signature VARCHAR,
#             track_genre VARCHAR
#         );
#         """,
#         """
#         CREATE TABLE IF NOT EXISTS transformed_streams (
#             user_id VARCHAR,
#             track_id VARCHAR,
#             listen_time TIMESTAMP,
#             hour INT
#         );
#         """
#     ]
#     with conn.cursor() as cur:
#         for query in queries:
#             logger.info(f"🛠 Executing: {query.split('(')[0].strip()}")
#             cur.execute(query)
#         conn.commit()

# # === Get all transformed stream files ===
# def get_transformed_stream_keys(bucket, prefix="transformed-data/"):
#     s3 = boto3.client("s3")
#     keys = []
#     paginator = s3.get_paginator("list_objects_v2")
#     for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
#         for obj in page.get("Contents", []):
#             key = obj["Key"]
#             if key.endswith("_transformed.csv") and "streams" in key:
#                 keys.append(key)
#     return keys

# # === COPY command wrapper ===
# def copy_from_s3(conn, table, s3_key, delimiter=",", region="us-east-1", header=True):
#     bucket = "music-etl-processed-data"
#     role_arn = os.getenv("REDSHIFT_IAM_ROLE")

#     copy_sql = f"""
#     COPY {table}
#     FROM 's3://{bucket}/{s3_key}'
#     IAM_ROLE '{role_arn}'
#     REGION '{region}'
#     FORMAT AS PARQUET;
#     """

#     with conn.cursor() as cur:
#         logger.info(f"🔄 Loading s3://{bucket}/{s3_key} into table `{table}`")
#         cur.execute(copy_sql)
#         conn.commit()
#         logger.info(f"✅ Loaded {s3_key} into {table}")

# # === Load everything ===
# def load_all():
#     try:
#         logger.info("🚀 Connecting to Redshift...")
#         conn = get_redshift_connection()

#         logger.info("📐 Creating tables if not present...")
#         create_tables(conn)

#         logger.info("📤 Loading raw users and songs...")
#         copy_from_s3(conn, "raw_users", "validated-data/validated_users.csv")
#         copy_from_s3(conn, "raw_songs", "validated-data/validated_songs.csv")

#         logger.info("📤 Loading all transformed stream files...")
#         stream_keys = get_transformed_stream_keys("music-etl-processed-data")
#         if not stream_keys:
#             logger.info("📭 No transformed stream files to load.")
#         for key in stream_keys:
#             copy_from_s3(conn, "transformed_streams", key)

#         conn.close()
#         logger.info("🏁 Redshift loading complete.")

#     except Exception as e:
#         logger.exception("❌ Redshift load failed.")
#         raise

# if __name__ == "__main__":
#     load_all()

