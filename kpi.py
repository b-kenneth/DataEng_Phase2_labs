import pandas as pd
import logging
from utils import read_csv_from_s3, write_csv_to_s3

# === Setup Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BUCKET = "music-etl-processed-data"
INPUT_KEY = "transformed-data/transformed_streams.csv"
GENRE_KPI_KEY = "KPIs-Computations/genre_kpis.csv"
HOURLY_KPI_KEY = "KPIs-Computations/hourly_kpis.csv"

def compute_genre_kpis(df: pd.DataFrame):
    logger.info("Computing genre-level KPIs...")

    grouped = df.groupby("track_genre").agg(
        listen_count=pd.NamedAgg(column="track_id", aggfunc="count"),
        avg_duration_sec=pd.NamedAgg(column="duration_sec", aggfunc="mean"),
        avg_popularity=pd.NamedAgg(column="popularity", aggfunc="mean")
    ).reset_index()

    grouped["popularity_index"] = grouped["avg_popularity"] * grouped["listen_count"]

    top_tracks = (
        df.sort_values("popularity", ascending=False)
        .groupby("track_genre")
        .first()
        .reset_index()[["track_genre", "track_name", "popularity"]]
        .rename(columns={"track_name": "most_popular_track", "popularity": "popularity_score"})
    )

    genre_kpis = pd.merge(grouped, top_tracks, on="track_genre")

    logger.info(f"Computed genre KPIs for {len(genre_kpis)} genres.")
    return genre_kpis

def compute_hourly_kpis(df: pd.DataFrame):
    logger.info("Computing hourly KPIs...")

    hourly_stats = df.groupby("hour").agg(
        unique_listeners=pd.NamedAgg(column="user_id", aggfunc=lambda x: x.nunique()),
        total_plays=pd.NamedAgg(column="track_id", aggfunc="count"),
        unique_tracks=pd.NamedAgg(column="track_id", aggfunc=lambda x: x.nunique())
    ).reset_index()

    hourly_stats["track_diversity_index"] = (
        hourly_stats["unique_tracks"] / hourly_stats["total_plays"]
    ).round(3)

    top_artists = (
        df.groupby(["hour", "artists"])
        .size()
        .reset_index(name="play_count")
        .sort_values(["hour", "play_count"], ascending=[True, False])
        .groupby("hour")
        .first()
        .reset_index()[["hour", "artists"]]
        .rename(columns={"artists": "top_artist"})
    )

    hourly_kpis = pd.merge(hourly_stats, top_artists, on="hour")
    logger.info(f"Computed hourly KPIs for {len(hourly_kpis)} time slots.")
    return hourly_kpis

def compute_kpis():
    try:
        logger.info("Starting KPI computation...")

        df = read_csv_from_s3(BUCKET, INPUT_KEY)
        if df.empty:
            logger.warning("⚠️ Transformed input data is empty. Skipping KPI computation.")
            return

        genre_kpis = compute_genre_kpis(df)
        hourly_kpis = compute_hourly_kpis(df)

        write_csv_to_s3(genre_kpis, BUCKET, GENRE_KPI_KEY)
        write_csv_to_s3(hourly_kpis, BUCKET, HOURLY_KPI_KEY)

        logger.info("KPI computations complete and saved to S3.")

    except Exception as e:
        logger.exception("Failed during KPI computation.")
        raise

if __name__ == "__main__":
    compute_kpis()
