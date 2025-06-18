import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

# Script generated for node Transform_bookings
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, when, lower, trim, datediff, current_date
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    # Convert the input DynamicFrame to a Spark DataFrame for transformation
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Apply the series of transformations to create the curated DataFrame
    df_transformed = df.withColumn(
        # 1. Standardize the 'source' column for consistency
        "source",
        lower(trim(col("source")))
    ).withColumn(
        # 2. Standardize the price to USD, similar to the bookings logic
        "price_usd",
        when(col("currency") == "EUR", col("price") * 1.15)
        .when(col("currency") == "INR", col("price") * 0.012)
        .otherwise(col("price"))  # Assume other currencies are already in USD
    ).withColumn(
        # 3. Engineer a 'listing_age_days' feature
        "listing_age_days",
        datediff(current_date(), col("listing_created_on"))
    )

    # Convert the transformed Spark DataFrame back to a DynamicFrame
    output_dyf = DynamicFrame.fromDF(df_transformed, glueContext, "output_dyf")

    # Return the result as a DynamicFrameCollection, ready for the next step
    return DynamicFrameCollection({"CustomTransform": output_dyf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node apartments_R
apartments_R_node1750164425740 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-992824239635-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "raw_data.apartments", "connectionName": "Redshift connection"}, transformation_ctx="apartments_R_node1750164425740")

# Script generated for node Drop Duplicates_bookings
DropDuplicates_bookings_node1750179309202 =  DynamicFrame.fromDF(apartments_R_node1750164425740.toDF().dropDuplicates(), glueContext, "DropDuplicates_bookings_node1750179309202")

# Script generated for node Transform_bookings
Transform_bookings_node1750181363056 = MyTransform(glueContext, DynamicFrameCollection({"DropDuplicates_bookings_node1750179309202": DropDuplicates_bookings_node1750179309202}, glueContext))

# Script generated for node Select From Bookings
SelectFromBookings_node1750181098644 = SelectFromCollection.apply(dfc=Transform_bookings_node1750181363056, key=list(Transform_bookings_node1750181363056.keys())[0], transformation_ctx="SelectFromBookings_node1750181098644")

# Script generated for node Apartments_curated
Apartments_curated_node1750180541106 = glueContext.write_dynamic_frame.from_options(frame=SelectFromBookings_node1750181098644, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-992824239635-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartments", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartments (id INTEGER, title VARCHAR, source VARCHAR, price DECIMAL, currency VARCHAR, listing_created_on TIMESTAMP, is_active BOOLEAN, last_modified_timestamp TIMESTAMP, price_usd DOUBLE PRECISION, listing_age_days INTEGER); TRUNCATE TABLE curated.apartments;"}, transformation_ctx="Apartments_curated_node1750180541106")

job.commit()