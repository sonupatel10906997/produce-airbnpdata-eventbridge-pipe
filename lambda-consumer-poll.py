import json
import boto3
import logging
from botocore.exceptions import ClientError
from datetime import datetime
import os

def lambda_handler(event, context):
    # TODO implement
    
   
    query_url = "https://sqs.us-east-2.amazonaws.com/975049912631/airbnb-booking-queue"
    s3_file = 'airbnb_booking_{}.json'.format(datetime.now().strftime('%Y%m%d%H%M%S'))
    s3_bucket = "airbnb-booking-records-growskills"
    
    try:
        sqs_client = boto3.client('sqs')
        s3client = boto3.client('s3')
        os.chdir('/tmp')
        
        response = sqs_client.receive_message( \
        QueueUrl = query_url, \
        #VisibilityTimeout = 10, \
        MaxNumberOfMessages = 10, \
        WaitTimeSeconds = 2 # adjusting for long polling receive message wait time
        )
        
        messages = response.get("Messages",[])
        print("Messages received in batch: {}".format(len(messages)))
        
        
        jsonObjects = list()
        for message in messages:
            jsonObjects.append(message['Body'])
            sqs_client.delete_message(QueueUrl = query_url, ReceiptHandle = message['ReceiptHandle'])
            print("message processed and deleted successfully with messageId :" + message['ReceiptHandle'])
    
        
        if len(jsonObjects) > 0 :
            f = open(s3_file, 'w+')
            f.write(json.dumps(jsonObjects, indent=4))
            f.close()
            
            response = s3client.upload_file( s3_file, s3_bucket, "processed/" + s3_file )
            print("airbnp booking records successfully uploaded name = {} with size :{} bytes".format(s3_file, os.path.getsize(s3_file)))

    except ClientError as e:
        logging.exception(e)
        
    except Exception as err:
        logging.exception(err)
    

    return {
        'statusCode': 200,
        'body': json.dumps('Airbnp booking process Successful')
    }
