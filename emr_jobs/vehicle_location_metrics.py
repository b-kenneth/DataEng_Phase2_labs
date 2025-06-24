from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import sys

def setup_logging():
    """Configure logging for the Spark application"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        # Create console handler
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
    
    return logger

def main():
    # Setup logging
    logger = setup_logging()
    logger.info("Starting Vehicle and Location Performance Metrics job")
    
    try:
        spark = SparkSession.builder \
            .appName("VehicleLocationMetrics") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Set Spark log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        
        # S3 bucket configuration
        bucket_name = "car-rental-data-buck1"
        logger.info(f"Using S3 bucket: {bucket_name}")
        
        # Read raw data from S3
        logger.info("Starting to read raw data from S3...")
        
        try:
            vehicles_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/raw/vehicles.csv")
            vehicle_count = vehicles_df.count()
            logger.info(f"Successfully loaded vehicles dataset with {vehicle_count} records")
        except Exception as e:
            logger.error(f"Failed to load vehicles dataset: {str(e)}")
            raise
        
        try:
            locations_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/raw/locations.csv")
            location_count = locations_df.count()
            logger.info(f"Successfully loaded locations dataset with {location_count} records")
        except Exception as e:
            logger.error(f"Failed to load locations dataset: {str(e)}")
            raise
        
        try:
            transactions_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/raw/rental_transactions.csv")
            transaction_count = transactions_df.count()
            logger.info(f"Successfully loaded transactions dataset with {transaction_count} records")
        except Exception as e:
            logger.error(f"Failed to load transactions dataset: {str(e)}")
            raise
        
        # Data type conversions and cleaning
        logger.info("🔧 Starting data processing and transformation...")
        
        try:
            transactions_df = transactions_df.withColumn("total_amount", col("total_amount").cast("double")) \
                                           .withColumn("rental_start_time", to_timestamp(col("rental_start_time"))) \
                                           .withColumn("rental_end_time", to_timestamp(col("rental_end_time"))) \
                                           .withColumn("pickup_location", col("pickup_location").cast("integer")) \
                                           .withColumn("dropoff_location", col("dropoff_location").cast("integer"))
            
            # Convert location_id to integer for proper joining
            locations_df = locations_df.withColumn("location_id", col("location_id").cast("integer"))
            logger.info("Data type conversions completed successfully")
        except Exception as e:
            logger.error(f"Failed during data type conversion: {str(e)}")
            raise
        
        # Calculate rental duration in hours
        try:
            transactions_df = transactions_df.withColumn(
                "rental_duration_hours",
                (unix_timestamp(col("rental_end_time")) - unix_timestamp(col("rental_start_time"))) / 3600
            )
            logger.info("Rental duration calculation completed")
        except Exception as e:
            logger.error(f"Failed to calculate rental duration: {str(e)}")
            raise
        
        # Join transactions with pickup locations
        logger.info("Starting data joins...")
        try:
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
            logger.info("Location join completed successfully")
        except Exception as e:
            logger.error(f"Failed during location join: {str(e)}")
            raise
        
        # Join with vehicles data
        try:
            final_enriched_df = pickup_enriched_df.join(vehicles_df, "vehicle_id", "left")
            enriched_count = final_enriched_df.count()
            logger.info(f"Vehicle join completed successfully. Final enriched dataset has {enriched_count} records")
        except Exception as e:
            logger.error(f"Failed during vehicle join: {str(e)}")
            raise
        
        # KPI 1: Location Performance Metrics
        logger.info("Calculating location performance metrics...")
        try:
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
            
            location_metrics_count = location_metrics.count()
            logger.info(f"Location metrics calculated for {location_metrics_count} locations")
        except Exception as e:
            logger.error(f"Failed to calculate location metrics: {str(e)}")
            raise
        
        # KPI 2: Vehicle Type Performance Metrics
        logger.info("Calculating vehicle type performance metrics...")
        try:
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
            
            vehicle_type_count = vehicle_type_metrics.count()
            logger.info(f"Vehicle type metrics calculated for {vehicle_type_count} vehicle types")
        except Exception as e:
            logger.error(f"Failed to calculate vehicle type metrics: {str(e)}")
            raise
        
        # KPI 3: Brand Performance Metrics
        logger.info("Calculating brand performance metrics...")
        try:
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
            
            brand_count = brand_metrics.count()
            logger.info(f"Brand metrics calculated for {brand_count} brands")
        except Exception as e:
            logger.error(f"Failed to calculate brand metrics: {str(e)}")
            raise
        
        # Write results to S3 in Parquet format
        logger.info("Writing processed data to S3...")
        
        try:
            location_metrics.coalesce(1).write.mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"s3://{bucket_name}/processed/location_metrics/")
            logger.info("Location metrics successfully written to S3")
        except Exception as e:
            logger.error(f"Failed to write location metrics: {str(e)}")
            raise
        
        try:
            vehicle_type_metrics.coalesce(1).write.mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"s3://{bucket_name}/processed/vehicle_type_metrics/")
            logger.info("Vehicle type metrics successfully written to S3")
        except Exception as e:
            logger.error(f"Failed to write vehicle type metrics: {str(e)}")
            raise
        
        try:
            brand_metrics.coalesce(1).write.mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"s3://{bucket_name}/processed/brand_metrics/")
            logger.info("Brand metrics successfully written to S3")
        except Exception as e:
            logger.error(f"Failed to write brand metrics: {str(e)}")
            raise
        
        # Show sample results
        logger.info("Displaying sample results...")
        print("Location Metrics Sample:")
        location_metrics.show(10, truncate=False)
        
        print("Vehicle Type Metrics Sample:")
        vehicle_type_metrics.show(10, truncate=False)
        
        print("Brand Metrics Sample:")
        brand_metrics.show(10, truncate=False)
        
        logger.info("Vehicle and Location metrics processing completed successfully!")
        
    except Exception as e:
        logger.critical(f"Critical error in main processing: {str(e)}")
        raise
    finally:
        try:
            spark.stop()
            logger.info("Spark session stopped successfully")
        except:
            logger.warning("Failed to stop Spark session cleanly")

if __name__ == "__main__":
    main()
