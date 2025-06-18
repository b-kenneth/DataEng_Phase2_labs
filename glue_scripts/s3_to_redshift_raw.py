import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node raw_bookings
raw_bookings_node1750160108107 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://rental-marketplace-datalake-l2/staging/bookings/"], "recurse": True}, transformation_ctx="raw_bookings_node1750160108107")

# Script generated for node raw_apartments
raw_apartments_node1750158262770 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://rental-marketplace-datalake-l2/staging/apartments/"], "recurse": True}, transformation_ctx="raw_apartments_node1750158262770")

# Script generated for node raw_user_viewings
raw_user_viewings_node1750160126893 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://rental-marketplace-datalake-l2/staging/user_viewings/"], "recurse": True}, transformation_ctx="raw_user_viewings_node1750160126893")

# Script generated for node raw_appartment_att
raw_appartment_att_node1750160152384 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://rental-marketplace-datalake-l2/staging/apartment_attributes/"], "recurse": True}, transformation_ctx="raw_appartment_att_node1750160152384")

# Script generated for node Raw_layer_bookings
Raw_layer_bookings_node1750160111576 = glueContext.write_dynamic_frame.from_options(frame=raw_bookings_node1750160108107, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-992824239635-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "raw_data.bookings", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raw_data.bookings (booking_id INTEGER, user_id INTEGER, apartment_id INTEGER, booking_date TIMESTAMP, checkin_date DATE, checkout_date DATE, total_price DECIMAL, currency VARCHAR, booking_status VARCHAR, payment_status VARCHAR, num_guests VARCHAR); TRUNCATE TABLE raw_data.bookings;"}, transformation_ctx="Raw_layer_bookings_node1750160111576")

# Script generated for node Raw_layer_apartments
Raw_layer_apartments_node1750158274022 = glueContext.write_dynamic_frame.from_options(frame=raw_apartments_node1750158262770, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-992824239635-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "raw_data.apartments", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raw_data.apartments (id INTEGER, title VARCHAR, source VARCHAR, price DECIMAL, currency VARCHAR, listing_created_on TIMESTAMP, is_active BOOLEAN, last_modified_timestamp TIMESTAMP); TRUNCATE TABLE raw_data.apartments;"}, transformation_ctx="Raw_layer_apartments_node1750158274022")

# Script generated for node Raw_layer_userviewings
Raw_layer_userviewings_node1750160143756 = glueContext.write_dynamic_frame.from_options(frame=raw_user_viewings_node1750160126893, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-992824239635-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "raw_data.user_viewings", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raw_data.user_viewings (user_id INTEGER, apartment_id INTEGER, viewed_at TIMESTAMP, is_wishlisted BOOLEAN, call_to_action VARCHAR); TRUNCATE TABLE raw_data.user_viewings;"}, transformation_ctx="Raw_layer_userviewings_node1750160143756")

# Script generated for node Raw_layer_apartmentAtt
Raw_layer_apartmentAtt_node1750160155199 = glueContext.write_dynamic_frame.from_options(frame=raw_appartment_att_node1750160152384, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-992824239635-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "raw_data.apartment_attributes", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raw_data.apartment_attributes (id INTEGER, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DECIMAL, has_photo BOOLEAN, pets_allowed BOOLEAN, price_display VARCHAR, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DECIMAL, longitude DECIMAL); TRUNCATE TABLE raw_data.apartment_attributes;"}, transformation_ctx="Raw_layer_apartmentAtt_node1750160155199")

job.commit()