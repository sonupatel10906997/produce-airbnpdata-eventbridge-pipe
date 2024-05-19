import json
import boto3
import pandas as pd
#import load_dotenv
import os
import io
from datetime import datetime
from botocore.exceptions import ClientError



def lambda_handler(event, context):
    
    #check if the message is empty 
    message = json.loads(event[0]['message'])
    print(type(message), message)
    if message == {}:
        return {
            'statusCode' : "201",
            'message' : "Empty Record"
        }
    
    # TODO implement
    S3BUCKET= 'airbnb-booking-records-growskills'
    todayDate = datetime.now().strftime('%Y%m%d')
    OBJECTKEY = f"{todayDate}/airbnp_processed_{todayDate}.csv"
    
    s3client = boto3.client('s3')

    
    try:
        response = s3client.get_object(Bucket=S3BUCKET, Key=OBJECTKEY)
        if response.get('ResponseMetadata', {}).get("HTTPStatusCode", 0) == 200:
            print(f"{OBJECTKEY} successfully retrieved")
        
        filecontent = response['Body'].read().decode('utf-8')
        # changing streaming obj to fileobj
        df = pd.read_csv(io.StringIO(filecontent), header ='infer', index_col=["bookingId"])
        
        df.loc[message["bookingId"]] = message["usrId"], message["propertyId"], message["location"], \
        message["startDate"], message["endDate"], message["price"]
        
        df.to_csv('/tmp/airbnb_temp.csv', header=True, index = True , encoding = 'utf-8')

        #uploading the file
        s3client.upload_file('/tmp/airbnb_temp.csv', S3BUCKET, OBJECTKEY)
        print('file appended successfully')
        
    except ClientError as e:
        if e.response['Error']['Code'] ==  "NoSuchKey":
            print(f"Exception !! File not found, Lets create a new file: {OBJECTKEY.split('/')[1]}")
        
        df_new = pd.DataFrame(columns=['bookingId', 'usrId', 'propertyId', 'location','startDate','endDate','price'])
        df_new.set_index(list(df_new.columns)[0], inplace = True)
        df_new.loc[message["bookingId"]] = message["usrId"], message["propertyId"], message["location"], message["startDate"], message["endDate"], message["price"]
        df_new.to_csv('/tmp/airbnb_temp.csv', index= True, header = True, encoding = 'utf-8')
        s3client.upload_file('/tmp/airbnb_temp.csv', S3BUCKET, OBJECTKEY)

        
    except Exception as e:
        print('Exception !! Please check the details', e)
    
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('File Successfully uploaded')
    }
