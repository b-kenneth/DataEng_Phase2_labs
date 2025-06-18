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

# Script generated for node Transform_apartments-att
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, when, lower, trim, regexp_replace
    from pyspark.sql.types import DecimalType
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

    # Convert the input DynamicFrame to a Spark DataFrame for easier manipulation
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Apply the series of transformations
    df_transformed = df.withColumn(
        # 1. Clean the 'price_display' column by removing the '$' and casting to a numeric type
        "price_display",
        regexp_replace(col("price_display"), "[$,]", "").cast(DecimalType(10, 2))
    ).withColumn(
        # 2. Engineer a new feature for cost analysis: fee per square foot
        # Includes a check to prevent division-by-zero errors
        "cost_per_sq_foot",
        when(col("square_feet").isNotNull() & (col("square_feet") > 0), 
             col("fee") / col("square_feet")
        ).otherwise(None)
    ).withColumn(
        # 3. Standardize the 'category' text field
        "category",
        lower(trim(col("category")))
    ).withColumn(
        # 4. Standardize the 'price_type' text field
        "price_type",
        lower(trim(col("price_type")))
    )

    # Convert the transformed Spark DataFrame back to a DynamicFrame
    output_dyf = DynamicFrame.fromDF(df_transformed, glueContext, "output_dyf")

    # Return the result as a DynamicFrameCollection, ready for the next node (e.g., loading to S3)
    return DynamicFrameCollection({"CustomTransform": output_dyf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node appartments-att_R
appartmentsatt_R_node1750164425740 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-992824239635-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "raw_data.apartment_attributes", "connectionName": "Redshift connection"}, transformation_ctx="appartmentsatt_R_node1750164425740")

# Script generated for node Drop Duplicates_apartments-att
DropDuplicates_apartmentsatt_node1750179309202 =  DynamicFrame.fromDF(appartmentsatt_R_node1750164425740.toDF().dropDuplicates(), glueContext, "DropDuplicates_apartmentsatt_node1750179309202")

# Script generated for node Transform_apartments-att
Transform_apartmentsatt_node1750181363056 = MyTransform(glueContext, DynamicFrameCollection({"DropDuplicates_apartmentsatt_node1750179309202": DropDuplicates_apartmentsatt_node1750179309202}, glueContext))

# Script generated for node Select From Bookings
SelectFromBookings_node1750181098644 = SelectFromCollection.apply(dfc=Transform_apartmentsatt_node1750181363056, key=list(Transform_apartmentsatt_node1750181363056.keys())[0], transformation_ctx="SelectFromBookings_node1750181098644")

# Script generated for node apartments-att_curated
apartmentsatt_curated_node1750180541106 = glueContext.write_dynamic_frame.from_options(frame=SelectFromBookings_node1750181098644, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-992824239635-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartment_attributes", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartment_attributes (id INTEGER, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DECIMAL, has_photo BOOLEAN, pets_allowed BOOLEAN, price_display DECIMAL, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DECIMAL, longitude DECIMAL, cost_per_sq_foot DECIMAL); TRUNCATE TABLE curated.apartment_attributes;"}, transformation_ctx="apartmentsatt_curated_node1750180541106")

job.commit()