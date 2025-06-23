from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    spark = SparkSession.builder \
        .appName("VehicleLocationMetrics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Replace with your actual bucket name
    bucket_name = "your-bucket-name"
    
    # Read raw data from S3
    print("📖 Reading raw data from S3...")
    vehicles_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/raw/vehicles.csv")
    locations_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/raw/locations.csv")
    transactions_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/raw/rental_transactions.csv")
    
    # Data type conversions and cleaning
    print("🔧 Processing and transforming data...")
    transactions_df = transactions_df.withColumn("total_amount", col("total_amount").cast("double")) \
                                   .withColumn("rental_start_time", to_timestamp(col("rental_start_time"))) \
                                   .withColumn("rental_end_time", to_timestamp(col("rental_end_time")))
    
    # Calculate rental duration in hours
    transactions_df = transactions_df.withColumn(
        "rental_duration_hours",
        (unix_timestamp(col("rental_end_time")) - unix_timestamp(col("rental_start_time"))) / 3600
    )
    
    # Join transactions with locations and vehicles
    enriched_df = transactions_df \
        .join(locations_df, transactions_df.pickup_location_id == locations_df.location_id, "left") \
        .join(vehicles_df, transactions_df.vehicle_id == vehicles_df.vehicle_id, "left")
    
    # KPI 1: Location Performance Metrics
    print("📊 Calculating location performance metrics...")
    location_metrics = enriched_df.groupBy("pickup_location_id", "location_name") \
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("*").alias("total_transactions"),
            avg("total_amount").alias("avg_transaction_amount"),
            max("total_amount").alias("max_transaction_amount"),
            min("total_amount").alias("min_transaction_amount"),
            countDistinct("vehicle_id").alias("unique_vehicles_used"),
            sum("rental_duration_hours").alias("total_rental_hours")
        ) \
        .orderBy(desc("total_revenue"))
    
    # KPI 2: Vehicle Type Performance Metrics
    print("🚗 Calculating vehicle type performance metrics...")
    vehicle_type_metrics = enriched_df.groupBy("vehicle_type") \
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("*").alias("total_transactions"),
            avg("rental_duration_hours").alias("avg_rental_duration"),
            sum("rental_duration_hours").alias("total_rental_hours"),
            avg("total_amount").alias("avg_transaction_amount")
        ) \
        .orderBy(desc("total_revenue"))
    
    # Write results to S3 in Parquet format
    print("💾 Writing processed data to S3...")
    location_metrics.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket_name}/processed/location_metrics/")
    
    vehicle_type_metrics.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket_name}/processed/vehicle_type_metrics/")
    
    # Show sample results
    print("📋 Location Metrics Sample:")
    location_metrics.show(10, truncate=False)
    
    print("📋 Vehicle Type Metrics Sample:")
    vehicle_type_metrics.show(10, truncate=False)
    
    print("✅ Vehicle and Location metrics processing completed!")
    spark.stop()

if __name__ == "__main__":
    main()
