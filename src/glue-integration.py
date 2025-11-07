
import logging
import os
import sys
import traceback
from datetime import datetime

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from commonPythonlib import readWriteOptions
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql import Row
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType

os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
has_sent_notification_log = False
collection_name = "temp_glue_integration_test_poc"
secret_key = "scdhhs-mes-dev/docdb-blue"
bucket = "scdhhs-mes-platform-team"
key = "integrationTestPoc/member_fixed.txt"


def read_from_documentDB():
    try:
        job = Job(glue_context)
        job.init(glue_args['JOB_NAME'], glue_args)
        pipeline = ""
        json_array = readWriteOptions.read_from_documentDB_as_dataFrame_multi_partitions(glue_context, str(pipeline),
                                                                                         secret_key, collection_name,"canonical", log)
        return json_array

    except Exception as e:
        log.error(traceback.format_exc())
        log.error(f"Error occurred while reading from documentDB :: {e}")
        raise Exception(f"Error occurred while reading from documentDB. Further details available in Cloudwatch.")

def write_to_documentDB(lines: list[str]):
    try:
        job = Job(glue_context)
        job.init(glue_args['JOB_NAME'], glue_args)

        dyf_members = build_dynamic_frame_from_fixed_width(glue_context, lines)
        readWriteOptions.write_to_documentDB(glue_context, dyf_members, secret_key, collection_name ,
                                                 "canonical", log)
        log.info(f"Completed writing to {collection_name} reference transformed documents to DocumentDB")
        job.commit()
    except Exception as e:
        log.error(traceback.format_exc())
        log.error(f"Error occurred while writing to documentDB :: {e}")
        raise Exception(f'Error occurred while writing to document DB.Further details available in CloudWatch.')




# Setup logging
def setup_logger():
    LOG_LEVEL = logging.INFO
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    log = logging.getLogger()
    log.setLevel(LOG_LEVEL)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(LOG_LEVEL)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    log.addHandler(handler)
    return log





def safe_parse_date(date_str: str):
    cleaned = date_str.strip()
    if len(cleaned) != 8 or not cleaned.isdigit():
        return None
    try:
        return datetime.strptime(cleaned, "%Y%m%d").date()
    except ValueError:
        return None

def build_dynamic_frame_from_fixed_width(glue_context, lines):
    parsed_records = []
    for idx, line in enumerate(lines, start=1):
        # Defensive slicing — pad to minimum length
        line = line.ljust(34)

        dob_str = line[16:24]
        dob = safe_parse_date(dob_str)

        record = {
            "member_id": line[0:4].strip(),
            "name": line[4:16].strip(),
            "dob": dob,
            "gender": line[24:25].strip(),
            "policy_number": line[25:34].strip(),
        }

        # Optional: log malformed lines
        if dob is None:
            print(f"Warning: Invalid DOB in line {idx}: '{dob_str}' -> Skipped or set to None")

        parsed_records.append(Row(**record))

    schema = StructType([
        StructField("member_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("dob", DateType(), True),
        StructField("gender", StringType(), True),
        StructField("policy_number", StringType(), True),
    ])

    spark = glue_context.spark_session
    df = spark.createDataFrame(parsed_records, schema=schema)
    return DynamicFrame.fromDF(df, glue_context, "member_dynamic_frame")






try:


    log = setup_logger()
    glue_args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                              'pipelineConfigParam', 'awsSDKTries'])


except Exception as e:
    log.error(traceback.format_exc())
    log.error(
        'SCDHHS-ERROR::MEMBER Configuration error. Unable to start the Glue job. Please check the stacktrace.')


if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark)
    spark = SparkSession.builder.appName('GlueIntegrationTestPoc').master("local[*]").getOrCreate()

    try:
        # Read file
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        lines = response['Body'].read().decode('utf-8').splitlines()
        log.info(f"Lines from file : {lines}")
        # write
        write_to_documentDB(lines)
        # read
        db_df = read_from_documentDB()
        db_df.show(5)
        # log.info(f"Successfully read from collection. document size : {db_df.size}")

    except Exception as e:
        log.error(traceback.format_exc())
