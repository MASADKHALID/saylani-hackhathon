import json
import requests
import boto3
from datetime import datetime

s3 = boto3.client('s3')
sns = boto3.client('sns')

def lambda_handler(event, context):
    file_date = datetime.utcnow().strftime('%Y-%m-%d')
    bucket_name = "data-hackathon-smit-muhammad-asad-khalid"   # Your bucket
    data_key = f'raw/coinmarketcap/{file_date}.json'
    metadata_key = f'raw/coinmarketcap/metadata{file_date}.json'
    sns_topic_arn = 'arn:aws:sns:us-east-1:376129860326:sns'  # Your SNS topic ARN

    url = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing"
    params = {
        "start": "1",
        "limit": "10",
        "sortBy": "market_cap",
        "sortType": "desc",
        "convert": "USD",
        "cryptoType": "all",
        "tagType": "all",
        "audited": "false"
    }
    headers = {
        "Accepts": "application/json",
        "User-Agent": "Mozilla/5.0"
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        data = response.json()

        cryptos = []
        for crypto in data["data"]["cryptoCurrencyList"]:
            cryptos.append({
                "name": crypto["name"],
                "symbol": crypto["symbol"],
                "price": round(crypto["quotes"][0]["price"], 2),
                "market_cap": round(crypto["quotes"][0]["marketCap"])
            })

        # Upload main data JSON
        s3.put_object(
            Bucket=bucket_name,
            Key=data_key,
            Body=json.dumps(cryptos),
            ContentType="application/json"
        )

        # --- ADDING METADATA JSON UPLOAD ---
        metadata = {
            "source": "CoinMarketCap",
            "uploaded_at": datetime.utcnow().isoformat(),
            "data_file": data_key
        }
        s3.put_object(
            Bucket=bucket_name,
            Key=metadata_key,
            Body=json.dumps(metadata),
            ContentType="application/json"
        )

        # Optional: Publish SNS message (remove if not needed)
        sns.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(metadata),
            MessageAttributes={
                'source': {
                    'DataType': 'String',
                    'StringValue': metadata['source']
                }
            }
        )

        print(f"Data and metadata uploaded, SNS notification sent for source {metadata['source']}")

        return {"statusCode": 200, "body": "Success"}

    except Exception as e:
        print("Error:", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
