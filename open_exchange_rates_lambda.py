import json
import boto3
import requests
from datetime import datetime

s3 = boto3.client('s3')
sns = boto3.client('sns')

API_URL = "https://openexchangerates.org/api/latest.json"
APP_ID = "37bd5c443e2841be8354124777525a1d"
BUCKET_NAME = "data-hackathon-smit-muhammad-asad-khalid" 
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:376129860326:sns" 

def lambda_handler(event, context):
    try:
        # Step 1: Fetch exchange rates
        response = requests.get(f"{API_URL}?app_id={APP_ID}")
        response.raise_for_status()
        data = response.json()
        print(data)
        # Step 2: Generate S3 object keys
        date_str = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        data_key = f"raw/openexchangerates/{date_str}.json"
        metadata_key = f"raw/openexchangerates/metadata{date_str}.json"

        # Step 3: Upload data to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=data_key,
            Body=json.dumps(data),
            ContentType="application/json"
        )

        # --- ADDING METADATA JSON UPLOAD ---
        metadata = {
            "source": "openexchangerates",
            "uploaded_at": datetime.utcnow().isoformat(),
            "data_file": data_key
        }
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=metadata_key,
            Body=json.dumps(metadata),
            ContentType="application/json"
        )

        # Optional: Publish SNS message
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(metadata),
            MessageAttributes={
                'source': {
                    'DataType': 'String',
                    'StringValue': metadata['source']
                }
            }
        )

        return {
            "statusCode": 200,
            "body": f"Data saved to {data_key}, metadata saved to {metadata_key}"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }

