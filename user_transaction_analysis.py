from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    spark = SparkSession.builder \
        .appName("UserTransactionAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Replace with your actual bucket name
    bucket_name = "your-bucket-name"
    
    # Read raw data from S3
    print("📖 Reading raw data from S3...")
    users_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/raw/users.csv")
    transactions_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/raw/rental_transactions.csv")
    vehicles_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/raw/vehicles.csv")
    
    # Data type conversions
    print("🔧 Processing and transforming data...")
    transactions_df = transactions_df.withColumn("total_amount", col("total_amount").cast("double")) \
                                   .withColumn("rental_start_time", to_timestamp(col("rental_start_time"))) \
                                   .withColumn("rental_end_time", to_timestamp(col("rental_end_time")))
    
    # Convert user creation_date and is_active fields
    users_df = users_df.withColumn("creation_date", to_date(col("creation_date"))) \
                       .withColumn("is_active", col("is_active").cast("integer")) \
                       .withColumn("driver_license_expiry", to_date(col("driver_license_expiry")))
    
    # Calculate rental duration in hours
    transactions_df = transactions_df.withColumn(
        "rental_duration_hours",
        (unix_timestamp(col("rental_end_time")) - unix_timestamp(col("rental_start_time"))) / 3600
    )
    
    # Extract date and time components from rental start time
    transactions_df = transactions_df.withColumn("rental_date", to_date(col("rental_start_time"))) \
                                   .withColumn("rental_hour", hour(col("rental_start_time"))) \
                                   .withColumn("rental_day_of_week", dayofweek(col("rental_start_time"))) \
                                   .withColumn("rental_month", month(col("rental_start_time")))
    
    # KPI 1: Daily Transaction Metrics
    print("📅 Calculating daily transaction metrics...")
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
    
    # KPI 2: Hourly Transaction Patterns
    print("⏰ Calculating hourly transaction patterns...")
    hourly_metrics = transactions_df.groupBy("rental_hour") \
        .agg(
            count("*").alias("total_transactions"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_transaction_amount")
        ) \
        .orderBy("rental_hour")
    
    # Join with users data for user analysis
    user_transactions = transactions_df.join(users_df, "user_id", "left")
    
    # KPI 3: User Engagement Metrics
    print("👥 Calculating user engagement metrics...")
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
    
    # KPI 4: Monthly Transaction Trends
    print("📊 Calculating monthly transaction trends...")
    monthly_metrics = transactions_df.groupBy("rental_month") \
        .agg(
            count("*").alias("total_transactions"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_transaction_amount"),
            countDistinct("user_id").alias("unique_users"),
            sum("rental_duration_hours").alias("total_rental_hours")
        ) \
        .orderBy("rental_month")
    
    # KPI 5: Active vs Inactive User Analysis
    print("🔍 Analyzing active vs inactive users...")
    user_activity_metrics = user_transactions.groupBy("is_active") \
        .agg(
            count("*").alias("total_transactions"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_transaction_amount"),
            countDistinct("user_id").alias("unique_users"),
            sum("rental_duration_hours").alias("total_rental_hours")
        )
    
    # Write results to S3 in Parquet format
    print("💾 Writing processed data to S3...")
    daily_metrics.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket_name}/processed/daily_metrics/")
    
    hourly_metrics.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket_name}/processed/hourly_metrics/")
    
    user_metrics.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket_name}/processed/user_metrics/")
    
    monthly_metrics.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket_name}/processed/monthly_metrics/")
    
    user_activity_metrics.coalesce(1).write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3://{bucket_name}/processed/user_activity_metrics/")
    
    # Show sample results
    print("📋 Daily Metrics Sample:")
    daily_metrics.show(10, truncate=False)
    
    print("📋 User Metrics Sample:")
    user_metrics.show(10, truncate=False)
    
    print("📋 Hourly Patterns Sample:")
    hourly_metrics.show(24, truncate=False)
    
    print("📋 User Activity Analysis:")
    user_activity_metrics.show(truncate=False)
    
    print("✅ User and Transaction analysis completed!")
    spark.stop()

if __name__ == "__main__":
    main()
