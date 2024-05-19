import json
import random
from datetime import datetime, timedelta
import boto3

def lambda_handler(event, context):
    QUEUE_URL = "https://sqs.us-east-2.amazonaws.com/975049912631/airbnb-booking-queue"
    sqs_client = boto3.client('sqs')

    # No of mock data to generate
    sample_size = 2
    location = ["Srinagar, India", "Paris, France" "Pondycherry, India", "Miami, USA", "Warsaw, Poland"]

    for counter in range(sample_size):
        start_date = datetime.now() + timedelta(random.randint(-3,0))
        end_date = start_date + timedelta(random.randint(0,7))

        booking_item = {
            "bookingId": "UUID-{}".format(random.randint(1,10000)),
            "usrId" : random.randint(1000,100000),
            "propertyId" : random.randint(10000,100000),
            "location" : random.choice(location),
            "startDate" : start_date.strftime('%Y-%m-%d'),
            "endDate" : end_date.strftime('%Y-%m-%d'),
            "price" : random.randint(50,500)
            }
        
        response = sqs_client.send_message( \
            QueueUrl = QUEUE_URL, \
            MessageAttributes = {
                "receivedDate" : {
                    'DataType' : 'Date',
                    'StringValue' : datetime.now().strftime('%Y-%m-%d')
                },
                "RealData" : {
                    'DataType' : 'String',
                    'StringValue' : 'mock'
                }
            },
            DelaySeconds = 1, # seconds to wait before make message visible to consumers
            MessageBody = json.dumps(booking_item) )
        
        print(response.get('MessageId'))
    

    return {
        "status": 200,
        "body": json.dumps("Airbnb data successfully generated for {} users".format(len(sample_size)))
    }

# lambda_handler([],[])