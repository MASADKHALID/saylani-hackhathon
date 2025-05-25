import json
import boto3
import csv
import io
from datetime import datetime
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get bucket name from environment variable or hardcode it here
    bucket = os.environ.get('BUCKET_NAME', 'data-hackathon-smit-muhammad-asad-khalid')

    # SQS event contains SNS notification as string in 'body'
    record = event['Records'][0]
    sns_message_str = record['body']
    
    # Parse the outer SNS message
    sns_message = json.loads(sns_message_str)
    
    # The actual message is a JSON string inside SNS 'Message'
    message_str = sns_message['Message']
    message = json.loads(message_str)
    
    # Get S3 object key from message metadata
    key = message.get('data_file')
    print(f"Bucket: {bucket}")
    print(f"Key: {key}")
    
    # Read file content from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    
    # Parse JSON content
    try:
        data = json.loads(content)
    except Exception as e:
        return {"statusCode": 400, "body": f"JSON parse error: {str(e)}"}

    if not isinstance(data, list) or not data:
        return {"statusCode": 400, "body": "Expected a list of dictionaries."}

    # Convert JSON to CSV
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)

    # Create file name using today's date
    today_str = datetime.now().strftime('%Y-%m-%d')
    processed_key = f"raw/coinmarketcap/processed/{today_str}.csv"

    # Upload CSV back to S3
    s3.put_object(Bucket=bucket, Key=processed_key, Body=output.getvalue())

    return {
        "statusCode": 200,
        "body": f"CSV saved to {processed_key}"
    }

