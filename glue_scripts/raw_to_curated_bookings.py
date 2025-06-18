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
    from pyspark.sql.functions import col, when
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    # Get the input DynamicFrame (assuming only one input)
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Apply conversion logic
    df_converted = df.withColumn(
        "total_price_usd",
        when(col("currency") == "EUR", col("total_price") * 1.15)
        .when(col("currency") == "INR", col("total_price") * 0.012)
        .otherwise(col("total_price"))  # Assume already USD
    )

    # Convert back to DynamicFrame
    output_dyf = DynamicFrame.fromDF(df_converted, glueContext, "output_dyf")

    # Return as DynamicFrameCollection with same key
    return DynamicFrameCollection({"CustomTransform": output_dyf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node bookings_R
bookings_R_node1750164425740 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-992824239635-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "raw_data.bookings", "connectionName": "Redshift connection"}, transformation_ctx="bookings_R_node1750164425740")

# Script generated for node Drop Duplicates_bookings
DropDuplicates_bookings_node1750179309202 =  DynamicFrame.fromDF(bookings_R_node1750164425740.toDF().dropDuplicates(), glueContext, "DropDuplicates_bookings_node1750179309202")

# Script generated for node Transform_bookings
Transform_bookings_node1750181363056 = MyTransform(glueContext, DynamicFrameCollection({"DropDuplicates_bookings_node1750179309202": DropDuplicates_bookings_node1750179309202}, glueContext))

# Script generated for node Select From Bookings
SelectFromBookings_node1750181098644 = SelectFromCollection.apply(dfc=Transform_bookings_node1750181363056, key=list(Transform_bookings_node1750181363056.keys())[0], transformation_ctx="SelectFromBookings_node1750181098644")

# Script generated for node Bookings_curated
Bookings_curated_node1750180541106 = glueContext.write_dynamic_frame.from_options(frame=SelectFromBookings_node1750181098644, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-992824239635-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "curated.bookings", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.bookings (booking_id INTEGER, user_id INTEGER, apartment_id INTEGER, booking_date TIMESTAMP, checkin_date DATE, checkout_date DATE, total_price DECIMAL, currency VARCHAR, booking_status VARCHAR, payment_status VARCHAR, num_guests VARCHAR, total_price_usd DOUBLE PRECISION); TRUNCATE TABLE curated.bookings;"}, transformation_ctx="Bookings_curated_node1750180541106")

job.commit()