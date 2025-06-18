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

# Script generated for node Transform_user
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, lower, trim, year, month, date_format
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    # Convert the input DynamicFrame to a Spark DataFrame for transformation
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Apply the series of transformations
    df_transformed = df.withColumn(
        # 1. Standardize the 'call_to_action' text field for consistency
        "call_to_action",
        lower(trim(col("call_to_action")))
    ).withColumn(
        # 2. Engineer time-based features from the 'viewed_at' timestamp
        "viewing_year",
        year(col("viewed_at"))
    ).withColumn(
        "viewing_month",
        month(col("viewed_at"))
    ).withColumn(
        # 'E' format gives the day of the week as a short string (e.g., 'Mon', 'Tue')
        "viewing_day_of_week",
        date_format(col("viewed_at"), "EEEE")
    )

    # Convert the transformed Spark DataFrame back into a DynamicFrame
    output_dyf = DynamicFrame.fromDF(df_transformed, glueContext, "output_dyf")

    # Return the result in a DynamicFrameCollection for the next stage of the job
    return DynamicFrameCollection({"CustomTransform": output_dyf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node user-viweings_R
userviweings_R_node1750164425740 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-992824239635-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "raw_data.user_viewings", "connectionName": "Redshift connection"}, transformation_ctx="userviweings_R_node1750164425740")

# Script generated for node Drop Duplicates_user
DropDuplicates_user_node1750179309202 =  DynamicFrame.fromDF(userviweings_R_node1750164425740.toDF().dropDuplicates(), glueContext, "DropDuplicates_user_node1750179309202")

# Script generated for node Transform_user
Transform_user_node1750181363056 = MyTransform(glueContext, DynamicFrameCollection({"DropDuplicates_user_node1750179309202": DropDuplicates_user_node1750179309202}, glueContext))

# Script generated for node Select From Bookings
SelectFromBookings_node1750181098644 = SelectFromCollection.apply(dfc=Transform_user_node1750181363056, key=list(Transform_user_node1750181363056.keys())[0], transformation_ctx="SelectFromBookings_node1750181098644")

# Script generated for node user-viewings_curated
userviewings_curated_node1750180541106 = glueContext.write_dynamic_frame.from_options(frame=SelectFromBookings_node1750181098644, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-992824239635-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "curated.user_viewings", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.user_viewings (user_id INTEGER, apartment_id INTEGER, viewed_at TIMESTAMP, is_wishlisted BOOLEAN, call_to_action VARCHAR, viewing_year INTEGER, viewing_month INTEGER, viewing_day_of_week VARCHAR); TRUNCATE TABLE curated.user_viewings;"}, transformation_ctx="userviewings_curated_node1750180541106")

job.commit()