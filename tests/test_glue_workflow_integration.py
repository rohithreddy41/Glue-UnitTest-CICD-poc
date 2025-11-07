import json
import os
import time

import boto3
import pytest

LAMBDA_ARN = 'arn:aws:lambda:us-east-1:549676063696:function:documentDBAccessFromSharedAccount'

# Environment variables required
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME", "glue_integration_test_poc")
DOCDB_URI = """mongodb://username:password@your-docdb-endpoint:27017/?ssl=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"""

# Polling intervals
WAIT_INTERVAL = 30  # seconds
MAX_WAIT = 1800  # 30 minutes

@pytest.fixture(scope="module")
def glue_client():
    return boto3.client("glue", region_name=AWS_REGION)


def start_glue_job(glue_client):
    print(f"Starting Glue Job: {GLUE_JOB_NAME} ")
    response = glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
    )
    run_id = response["JobRunId"]
    print(f"Started Glue job run ID: {run_id}")
    return run_id


def wait_for_job_completion(glue_client, run_id):
    start_time = time.time()
    while time.time() - start_time < MAX_WAIT:
        response = glue_client.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id, PredecessorsIncluded=False)
        state = response["JobRun"]["JobRunState"]
        print(f"Glue job status: {state}")
        if state in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"]:
            return state
        time.sleep(WAIT_INTERVAL)
    return "TIMEOUT"

def invoke_lambda(payload):
    client = boto3.client('lambda', region_name='us-east-1')
    response = client.invoke(
        FunctionName=LAMBDA_ARN,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload).encode()
    )
    result = json.loads(response['Payload'].read())
    assert result['statusCode'] == 200, f"Lambda error: {result['body']}"
    return json.loads(result['body'])




def test_glue_job_end_to_end(glue_client):

    assert DOCDB_URI, "Missing environment variable DOCDB_URI"

    # trigger
    # run_id = start_glue_job(glue_client)

    # wait
    # state = wait_for_job_completion(glue_client, run_id)
    # assert state == "SUCCEEDED", f"Glue job failed or incomplete. State: {state}"

    # verify
    payload = {
        'db': 'userdb',
        'collection': 'users',
        'query': {'status': 'active'}
    }
    results = invoke_lambda(payload)
    print(results)
    # assert isinstance(results, list)
    # assert all(user['status'] == 'active' for user in results)





    # client = pymongo.MongoClient(DOCDB_URI)
    # db = client["healthcare"]
    # coll = db["members"]
    # results = list(coll.find({}, {"_id": 0}))
    # client.close()
    #
    # print(f"DocumentDB results count: {len(results)}")
    # assert len(results) > 0, "No records found in DocumentDB"
    #
    # names = [r.get("name") for r in results]
    # assert "John Doe" in names, f"'John Doe' not found in DocumentDB collection."
    # assert "Jane Smith" in names, f"'Jane Smith' not found in DocumentDB collection."
    # print(f" Verified members in DocumentDB: {names}")
