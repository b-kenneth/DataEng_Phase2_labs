import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Rental-db
Rentaldb_node1750086101109 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "bookings",
        "connectionName": "aurora-rental-marketplace-connection",
    },
    transformation_ctx = "Rentaldb_node1750086101109"
)

# Script generated for node Rental-db
Rentaldb_node1750086235170 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartment_attributes",
        "connectionName": "aurora-rental-marketplace-connection",
    },
    transformation_ctx = "Rentaldb_node1750086235170"
)

# Script generated for node Rental-db
Rentaldb_node1750071718106 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartments",
        "connectionName": "aurora-rental-marketplace-connection",
    },
    transformation_ctx = "Rentaldb_node1750071718106"
)

# Script generated for node Rental-db
Rentaldb_node1750086213970 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "user_viewings",
        "connectionName": "aurora-rental-marketplace-connection",
    },
    transformation_ctx = "Rentaldb_node1750086213970"
)

# Script generated for node Staging bookings
EvaluateDataQuality().process_rows(frame=Rentaldb_node1750086101109, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750074167422", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Stagingbookings_node1750086106410 = glueContext.write_dynamic_frame.from_options(frame=Rentaldb_node1750086101109, connection_type="s3", format="glueparquet", connection_options={"path": "s3://rental-marketplace-datalake-l2/staging/bookings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Stagingbookings_node1750086106410")

# Script generated for node Staging apartment_attributes
EvaluateDataQuality().process_rows(frame=Rentaldb_node1750086235170, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750074167422", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Stagingapartment_attributes_node1750086260068 = glueContext.write_dynamic_frame.from_options(frame=Rentaldb_node1750086235170, connection_type="s3", format="glueparquet", connection_options={"path": "s3://rental-marketplace-datalake-l2/staging/apartment_attributes/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Stagingapartment_attributes_node1750086260068")

# Script generated for node Staging apartments
EvaluateDataQuality().process_rows(frame=Rentaldb_node1750071718106, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750069407918", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Stagingapartments_node1750071724476 = glueContext.write_dynamic_frame.from_options(frame=Rentaldb_node1750071718106, connection_type="s3", format="glueparquet", connection_options={"path": "s3://rental-marketplace-datalake-l2/staging/apartments/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Stagingapartments_node1750071724476")

# Script generated for node Staging user_viewings
EvaluateDataQuality().process_rows(frame=Rentaldb_node1750086213970, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750074167422", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Staginguser_viewings_node1750086218946 = glueContext.write_dynamic_frame.from_options(frame=Rentaldb_node1750086213970, connection_type="s3", format="glueparquet", connection_options={"path": "s3://rental-marketplace-datalake-l2/staging/user_viewings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Staginguser_viewings_node1750086218946")

job.commit()