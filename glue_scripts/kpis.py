#### 1. Average Listing Price (Weekly)

def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, avg, date_trunc
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Calculation assuming 'df' contains listing data.
    # We will use a separate input DataFrame for this specific KPI.
    df_listings = dfc.select('unified_listings').toDF() # Assumes a separate input for listings

    df_kpi = df_listings.filter(col("is_active") == True) \
        .withColumn("week_start_date", date_trunc('week', col("listing_created_on")).cast("date")) \
        .groupBy("week_start_date", "cityname") \
        .agg(avg("price_usd").alias("avg_price_usd"))

    output_dyf = DynamicFrame.fromDF(df_kpi, glueContext, "output_dyf")
    return DynamicFrameCollection({"CustomTransform": output_dyf}, glueContext)


#### 2. Occupancy Rate (Monthly)

def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, sum, year, month, dayofmonth, last_day, to_date, round as spark_round
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df_kpi = df.filter(col("booking_status") == "confirmed") \
        .withColumn("year", year(col("booking_date"))) \
        .withColumn("month", month(col("booking_date"))) \
        .groupBy("year", "month", "apartment_id", "cityname") \
        .agg(sum("booking_duration_days").alias("booked_nights")) \
        .withColumn("days_in_month", dayofmonth(last_day(to_date(col("year").cast("string") + "-" + col("month").cast("string") + "-01")))) \
        .withColumn("occupancy_rate_pct", spark_round((col("booked_nights") / col("days_in_month")) * 100, 2)) \
        .select("year", "month", "apartment_id", "cityname", "booked_nights", "occupancy_rate_pct")

    output_dyf = DynamicFrame.fromDF(df_kpi, glueContext, "output_dyf")
    return DynamicFrameCollection({"CustomTransform": output_dyf}, glueContext)


#### 3. Most Popular Locations (Weekly)

def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, count, date_trunc, rank
    from pyspark.sql.window import Window
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df_weekly_bookings = df.filter(col("booking_status") == "confirmed") \
        .withColumn("week_start_date", date_trunc('week', col("booking_date")).cast("date")) \
        .groupBy("week_start_date", "cityname") \
        .agg(count("booking_id").alias("booking_count"))

    windowSpec = Window.partitionBy("week_start_date").orderBy(col("booking_count").desc())
    df_kpi = df_weekly_bookings.withColumn("rank", rank().over(windowSpec))

    output_dyf = DynamicFrame.fromDF(df_kpi, glueContext, "output_dyf")
    return DynamicFrameCollection({"CustomTransform": output_dyf}, glueContext)


#### 4. Top Performing Listings (Weekly Revenue)

def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, sum, date_trunc
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df_kpi = df.filter(col("booking_status") == "confirmed") \
        .withColumn("week_start_date", date_trunc('week', col("booking_date")).cast("date")) \
        .groupBy("week_start_date", "apartment_id", "title") \
        .agg(sum("total_price_usd").alias("total_revenue_usd")) \
        .select("week_start_date", "apartment_id", "title", "total_revenue_usd")

    output_dyf = DynamicFrame.fromDF(df_kpi, glueContext, "output_dyf")
    return DynamicFrameCollection({"CustomTransform": output_dyf}, glueContext)


#### 5. Total Bookings per User (Weekly)

def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, count, date_trunc
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df_kpi = df.filter(col("booking_status") == "confirmed") \
        .withColumn("week_start_date", date_trunc('week', col("booking_date")).cast("date")) \
        .groupBy("week_start_date", "user_id") \
        .agg(count("booking_id").alias("total_bookings"))

    output_dyf = DynamicFrame.fromDF(df_kpi, glueContext, "output_dyf")
    return DynamicFrameCollection({"CustomTransform": output_dyf}, glueContext)


#### 6. Average Booking Duration (Monthly)

def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, avg, year, month
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df_kpi = df.filter(col("booking_status") == "confirmed") \
        .withColumn("year", year(col("booking_date"))) \
        .withColumn("month", month(col("booking_date"))) \
        .groupBy("year", "month") \
        .agg(avg("booking_duration_days").alias("avg_duration_days"))

    output_dyf = DynamicFrame.fromDF(df_kpi, glueContext, "output_dyf")
    return DynamicFrameCollection({"CustomTransform": output_dyf}, glueContext)


#### 7. Repeat Customer Rate (Monthly)

def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, count, lag, datediff, year, month, when, sum as spark_sum, round as spark_round
    from pyspark.sql.window import Window
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    df = dfc.select(list(dfc.keys())[0]).toDF()

    windowSpec = Window.partitionBy("user_id").orderBy("booking_date")
    df_with_lag = df.filter(col("booking_status") == "confirmed") \
        .withColumn("previous_booking_date", lag("booking_date", 1).over(windowSpec))

    df_repeat_flag = df_with_lag.withColumn("days_since_last_booking", datediff(col("booking_date"), col("previous_booking_date"))) \
        .withColumn("is_repeat_booking", when(col("days_since_last_booking") <= 30, 1).otherwise(0))

    df_kpi = df_repeat_flag.withColumn("year", year(col("booking_date"))) \
        .withColumn("month", month(col("booking_date"))) \
        .groupBy("year", "month") \
        .agg(
            count("booking_id").alias("total_bookings"),
            spark_sum("is_repeat_booking").alias("repeat_bookings")
        ) \
        .withColumn("repeat_customer_rate_pct", spark_round((col("repeat_bookings") / col("total_bookings")) * 100, 2).alias("repeat_customer_rate_pct"))

    output_dyf = DynamicFrame.fromDF(df_kpi, glueContext, "output_dyf")
    return DynamicFrameCollection({"CustomTransform": output_dyf}, glueContext)
