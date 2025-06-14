import psycopg2
import logging
from get_redshift_secret import get_secret  # helper to fetch from Secrets Manager

# === Setup Logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def get_redshift_connection():
    secret = get_secret("music-etl-secrets")
    return psycopg2.connect(
        dbname=secret["REDSHIFT_DB"],
        user=secret["REDSHIFT_USER"],
        password=secret["REDSHIFT_PASSWORD"],
        host=secret["REDSHIFT_HOST"],
        port=int(secret.get("REDSHIFT_PORT", 5439))
    )

def compute_genre_kpis(cur):
    logger.info("Computing genre-level KPIs...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS genre_kpis (
            track_genre VARCHAR,
            listen_count INT,
            avg_duration_sec FLOAT,
            avg_popularity FLOAT,
            popularity_index FLOAT,
            most_popular_track VARCHAR,
            popularity_score INT
        );
        DELETE FROM genre_kpis;

        INSERT INTO genre_kpis
        SELECT
            ts.track_genre,
            COUNT(*) AS listen_count,
            ROUND(AVG(ts.duration_ms / 1000.0), 2) AS avg_duration_sec,
            ROUND(AVG(ts.popularity), 2) AS avg_popularity,
            ROUND(AVG(ts.popularity) * COUNT(*), 2) AS popularity_index,
            most_popular.track_name AS most_popular_track,
            most_popular.popularity AS popularity_score
        FROM transformed_streams st
        JOIN transformed_songs ts ON st.track_id = ts.track_id
        JOIN (
            SELECT track_genre, track_name, popularity
            FROM (
                SELECT track_genre, track_name, popularity,
                       ROW_NUMBER() OVER (PARTITION BY track_genre ORDER BY popularity DESC) as rn
                FROM transformed_songs
            ) ranked
            WHERE rn = 1
        ) most_popular ON ts.track_genre = most_popular.track_genre
        GROUP BY ts.track_genre, most_popular.track_name, most_popular.popularity;
    """)
    logger.info("Genre-level KPIs computed.")

def compute_hourly_kpis(cur):
    logger.info("Computing hourly KPIs...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hourly_kpis (
            hour INT,
            unique_listeners INT,
            total_plays INT,
            unique_tracks INT,
            track_diversity_index FLOAT,
            top_artist VARCHAR
        );
        DELETE FROM hourly_kpis;

        INSERT INTO hourly_kpis
        WITH hourly_data AS (
            SELECT
                st.hour,
                st.user_id,
                st.track_id,
                ts.artists
            FROM transformed_streams st
            JOIN transformed_songs ts ON st.track_id = ts.track_id
        ),
        artist_rank AS (
            SELECT
                hour,
                artists,
                ROW_NUMBER() OVER (PARTITION BY hour ORDER BY COUNT(*) DESC) AS rn
            FROM hourly_data
            GROUP BY hour, artists
        )
        SELECT
            hour,
            COUNT(DISTINCT user_id) AS unique_listeners,
            COUNT(*) AS total_plays,
            COUNT(DISTINCT track_id) AS unique_tracks,
            ROUND(COUNT(DISTINCT track_id) * 1.0 / COUNT(*), 3) AS track_diversity_index,
            MAX(CASE WHEN rn = 1 THEN artists END) AS top_artist
        FROM hourly_data
        LEFT JOIN artist_rank USING (hour, artists)
        GROUP BY hour;
    """)
    logger.info("Hourly KPIs computed.")

def compute_kpis():
    try:
        conn = get_redshift_connection()
        with conn.cursor() as cur:
            compute_genre_kpis(cur)
            compute_hourly_kpis(cur)
        conn.commit()
        conn.close()
        logger.info("All KPIs computed and saved to Redshift.")
    except Exception as e:
        logger.exception("KPI computation failed.")
        raise

if __name__ == "__main__":
    compute_kpis()
