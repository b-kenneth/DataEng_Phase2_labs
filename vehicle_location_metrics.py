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
                                   .withColumn("rental_end_time", to_timestamp(col("rental_end_time"))) \
                                   .withColumn("pickup_location", col("pickup_location").cast("integer")) \
                                   .withColumn("dropoff_location", col("dropoff_location").cast("integer"))
    
    # Convert location_id to integer for proper joining
    locations_df = locations_df.withColumn("location_id", col("location_id").cast("integer"))
    
    # Calculate rental duration in hours
    transactions_df = transactions_df.withColumn(
        "rental_duration_hours",
        (unix_timestamp(col("rental_end_time")) - unix_timestamp(col("rental_start_time"))) / 3600
    )
    
    # Join transactions with pickup locations
    pickup_enriched_df = transactions_df \
        .join(locations_df.alias("pickup_loc"), 
              transactions_df.pickup_location == col("pickup_loc.location_id"), "left") \
        .select(
            col("rental_id"),
            col("user_id"),
            col("vehicle_id"),
            col("rental_start_time"),
            col("rental_end_time"),
            col("pickup_location"),
            col("dropoff_location"),
            col("total_amount"),
            col("rental_duration_hours"),
            col("pickup_loc.location_name").alias("pickup_location_name"),
            col("pickup_loc.city").alias("pickup_city"),
            col("pickup_loc.state").alias("pickup_state")
        )
    
    # Join with vehicles data
    final_enriched_df = pickup_enriched_df \
        .join(vehicles_df, "vehicle_id", "left")
    
    # KPI 1: Location Performance Metrics (by pickup location)
    print("📊 Calculating location performance metrics...")
    location_metrics = final_enriched_df.groupBy("pickup_location", "pickup_location_name", "pickup_city", "pickup_state") \
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("*").alias("total_transactions"),
            avg("total_amount").alias("avg_transaction_amount"),
            max("total_amount").alias("max_transaction_amount"),
            min("total_amount").alias("min_transaction_amount"),
            countDistinct("vehicle_id").alias("unique_vehicles_used"),
            sum("rental_duration_hours").alias("total_rental_hours"),
            avg("rental_duration_hours").alias("avg_rental_duration"),
            countDistinct("user_id").alias("unique_customers")
        ) \
        .filter(col("pickup_location").isNotNull()) \
        .orderBy(desc("total_revenue"))
    
    # KPI 2: Vehicle Type Performance Metrics
    print("🚗 Calculating vehicle type performance metrics...")
    vehicle_type_metrics = final_enriched_df.groupBy("vehicle_type", "brand") \
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("*").alias("total_transactions"),
            avg("rental_duration_hours").alias("avg_rental_duration"),
            sum("rental_duration_hours").alias("total_rental_hours"),
            avg("total_amount").alias("avg_transaction_amount"),
            max("total_amount").alias("max_transaction_amount"),
            min("total_amount").alias("min_transaction_amount"),
            countDistinct("user_id").alias("unique_customers")
        ) \
        .filter(col("vehicle_type").isNotNull()) \
        .orderBy(desc("total_revenue"))
    
    # KPI 3: Brand Performance Metrics
    print("🏷️ Calculating brand performance metrics...")
    brand_metrics = final_enriched_df.groupBy("brand") \
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("*").alias("total_transactions"),
            avg("total_amount").alias("avg_transaction_amount"),
            countDistinct("vehicle_type").alias("vehicle_types_offered"),
            countDistinct("vehicle_id").alias("total_vehicles"),
            sum("rental_duration_hours").alias("total_rental_hours")
        ) \
        .filter(col("brand").isNotNull()) \
        .orderBy(desc("total_revenue"))
    
    # Write results to S3 in Parquet format
    print("💾 Writing processed data to S3...")
    location_metrics.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket_name}/processed/location_metrics/")
    
    vehicle_type_metrics.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket_name}/processed/vehicle_type_metrics/")
    
    brand_metrics.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket_name}/processed/brand_metrics/")
    
    # Show sample results
    print("📋 Location Metrics Sample:")
    location_metrics.show(10, truncate=False)
    
    print("📋 Vehicle Type Metrics Sample:")
    vehicle_type_metrics.show(10, truncate=False)
    
    print("📋 Brand Metrics Sample:")
    brand_metrics.show(10, truncate=False)
    
    print("✅ Vehicle and Location metrics processing completed!")
    spark.stop()

if __name__ == "__main__":
    main()
