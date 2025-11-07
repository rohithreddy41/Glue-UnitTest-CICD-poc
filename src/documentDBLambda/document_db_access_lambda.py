import pymongo
import json
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        event_indent = json.dumps(event, indent=2)
        logger.info(f"Processing event : {event_indent}")
        # Connect to DocumentDB
        # client = pymongo.MongoClient(os.environ['DOCDB_URI'])
        # db = client[event['db']]
        # collection = db[event['collection']]
        #
        # # Run query
        # query = event.get('query', {})
        # results = list(collection.find(query))
        #
        # # Convert ObjectId to string
        # for doc in results:
        #     doc['_id'] = str(doc['_id'])
        sample_data = {
            "message": "Hello from Lambda!",
            "data": {
                "id": 123,
                "name": "Sample Item",
                "status": "active"
            }
        }

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps(sample_data)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
