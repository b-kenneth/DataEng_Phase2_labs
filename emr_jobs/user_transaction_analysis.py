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
    logger.info("Starting User and Transaction Analysis job")
    
    try:
        spark = SparkSession.builder \
            .appName("UserTransactionAnalysis") \
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
            users_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/raw/users.csv")
            user_count = users_df.count()
            logger.info(f"Successfully loaded users dataset with {user_count} records")
        except Exception as e:
            logger.error(f"Failed to load users dataset: {str(e)}")
            raise
        
        try:
            transactions_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/raw/rental_transactions.csv")
            transaction_count = transactions_df.count()
            logger.info(f"Successfully loaded transactions dataset with {transaction_count} records")
        except Exception as e:
            logger.error(f"Failed to load transactions dataset: {str(e)}")
            raise
        
        try:
            vehicles_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/raw/vehicles.csv")
            vehicle_count = vehicles_df.count()
            logger.info(f"Successfully loaded vehicles dataset with {vehicle_count} records")
        except Exception as e:
            logger.error(f"Failed to load vehicles dataset: {str(e)}")
            raise
        
        # Data type conversions
        logger.info("🔧 Starting data processing and transformation...")
        
        try:
            transactions_df = transactions_df.withColumn("total_amount", col("total_amount").cast("double")) \
                                           .withColumn("rental_start_time", to_timestamp(col("rental_start_time"))) \
                                           .withColumn("rental_end_time", to_timestamp(col("rental_end_time")))
            
            # Convert user creation_date and is_active fields
            users_df = users_df.withColumn("creation_date", to_date(col("creation_date"))) \
                               .withColumn("is_active", col("is_active").cast("integer")) \
                               .withColumn("driver_license_expiry", to_date(col("driver_license_expiry")))
            
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
        
        # Extract date and time components from rental start time
        try:
            transactions_df = transactions_df.withColumn("rental_date", to_date(col("rental_start_time"))) \
                                           .withColumn("rental_hour", hour(col("rental_start_time"))) \
                                           .withColumn("rental_day_of_week", dayofweek(col("rental_start_time"))) \
                                           .withColumn("rental_month", month(col("rental_start_time")))
            logger.info("Date and time component extraction completed")
        except Exception as e:
            logger.error(f"Failed to extract date/time components: {str(e)}")
            raise
        
        # KPI 1: Daily Transaction Metrics
        logger.info("Calculating daily transaction metrics...")
        try:
            daily_metrics = transactions_df.groupBy("rental_date") \
                .agg(
                    count("*").alias("total_transactions"),
                    sum("total_amount").alias("total_revenue"),
                    avg("total_amount").alias("avg_transaction_amount"),
                    max("total_amount").alias("max_transaction_amount"),
                    min("total_amount").alias("min_transaction_amount"),
                    countDistinct("user_id").alias("unique_users"),
                    countDistinct("vehicle_id").alias("unique_vehicles"),
                    sum("rental_duration_hours").alias("total_rental_hours"),
                    avg("rental_duration_hours").alias("avg_rental_duration")
                ) \
                .orderBy("rental_date")
            
            daily_count = daily_metrics.count()
            logger.info(f"Daily metrics calculated for {daily_count} days")
        except Exception as e:
            logger.error(f"Failed to calculate daily metrics: {str(e)}")
            raise
        
        # KPI 2: Hourly Transaction Patterns
        logger.info("Calculating hourly transaction patterns...")
        try:
            hourly_metrics = transactions_df.groupBy("rental_hour") \
                .agg(
                    count("*").alias("total_transactions"),
                    sum("total_amount").alias("total_revenue"),
                    avg("total_amount").alias("avg_transaction_amount")
                ) \
                .orderBy("rental_hour")
            logger.info("Hourly transaction patterns calculated successfully")
        except Exception as e:
            logger.error(f"Failed to calculate hourly metrics: {str(e)}")
            raise
        
        # Join with users data for user analysis
        logger.info("Joining transaction data with user data...")
        try:
            user_transactions = transactions_df.join(users_df, "user_id", "left")
            user_transaction_count = user_transactions.count()
            logger.info(f"User-transaction join completed with {user_transaction_count} records")
        except Exception as e:
            logger.error(f"Failed during user-transaction join: {str(e)}")
            raise
        
        # KPI 3: User Engagement Metrics
        logger.info("👥 Calculating user engagement metrics...")
        try:
            user_metrics = user_transactions.groupBy("user_id", "first_name", "last_name", "email", "is_active") \
                .agg(
                    count("*").alias("total_transactions"),
                    sum("total_amount").alias("total_spending"),
                    avg("total_amount").alias("avg_spending_per_transaction"),
                    max("total_amount").alias("max_spending"),
                    min("total_amount").alias("min_spending"),
                    sum("rental_duration_hours").alias("total_rental_hours"),
                    avg("rental_duration_hours").alias("avg_rental_duration"),
                    countDistinct("vehicle_id").alias("unique_vehicles_rented"),
                    min("rental_date").alias("first_rental_date"),
                    max("rental_date").alias("last_rental_date")
                ) \
                .orderBy(desc("total_spending"))
            
            user_metrics_count = user_metrics.count()
            logger.info(f"User engagement metrics calculated for {user_metrics_count} users")
        except Exception as e:
            logger.error(f"Failed to calculate user metrics: {str(e)}")
            raise
        
        # KPI 4: Monthly Transaction Trends
        logger.info("Calculating monthly transaction trends...")
        try:
            monthly_metrics = transactions_df.groupBy("rental_month") \
                .agg(
                    count("*").alias("total_transactions"),
                    sum("total_amount").alias("total_revenue"),
                    avg("total_amount").alias("avg_transaction_amount"),
                    countDistinct("user_id").alias("unique_users"),
                    sum("rental_duration_hours").alias("total_rental_hours")
                ) \
                .orderBy("rental_month")
            logger.info("Monthly transaction trends calculated successfully")
        except Exception as e:
            logger.error(f"Failed to calculate monthly metrics: {str(e)}")
            raise
        
        # KPI 5: Active vs Inactive User Analysis
        logger.info("Analyzing active vs inactive users...")
        try:
            user_activity_metrics = user_transactions.groupBy("is_active") \
                .agg(
                    count("*").alias("total_transactions"),
                    sum("total_amount").alias("total_revenue"),
                    avg("total_amount").alias("avg_transaction_amount"),
                    countDistinct("user_id").alias("unique_users"),
                    sum("rental_duration_hours").alias("total_rental_hours")
                )
            logger.info("User activity analysis completed successfully")
        except Exception as e:
            logger.error(f"Failed to calculate user activity metrics: {str(e)}")
            raise
        
        # Write results to S3 in Parquet format
        logger.info("Writing processed data to S3...")
        
        try:
            daily_metrics.coalesce(1).write.mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"s3://{bucket_name}/processed/daily_metrics/")
            logger.info("Daily metrics successfully written to S3")
        except Exception as e:
            logger.error(f"Failed to write daily metrics: {str(e)}")
            raise
        
        try:
            hourly_metrics.coalesce(1).write.mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"s3://{bucket_name}/processed/hourly_metrics/")
            logger.info("Hourly metrics successfully written to S3")
        except Exception as e:
            logger.error(f"Failed to write hourly metrics: {str(e)}")
            raise
        
        try:
            user_metrics.coalesce(1).write.mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"s3://{bucket_name}/processed/user_metrics/")
            logger.info("User metrics successfully written to S3")
        except Exception as e:
            logger.error(f"Failed to write user metrics: {str(e)}")
            raise
        
        try:
            monthly_metrics.coalesce(1).write.mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"s3://{bucket_name}/processed/monthly_metrics/")
            logger.info("Monthly metrics successfully written to S3")
        except Exception as e:
            logger.error(f"Failed to write monthly metrics: {str(e)}")
            raise
        
        try:
            user_activity_metrics.coalesce(1).write.mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"s3://{bucket_name}/processed/user_activity_metrics/")
            logger.info("User activity metrics successfully written to S3")
        except Exception as e:
            logger.error(f"Failed to write user activity metrics: {str(e)}")
            raise
        
        # Show sample results
        logger.info("Displaying sample results...")
        print("Daily Metrics Sample:")
        daily_metrics.show(10, truncate=False)
        
        print("User Metrics Sample:")
        user_metrics.show(10, truncate=False)
        
        print("Hourly Patterns Sample:")
        hourly_metrics.show(24, truncate=False)
        
        print("User Activity Analysis:")
        user_activity_metrics.show(truncate=False)
        
        logger.info("User and Transaction analysis completed successfully!")
        
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
