import json
import boto3
import yfinance as yf
import pandas as pd
from datetime import datetime

s3 = boto3.client('s3')
sns = boto3.client('sns')

def lambda_handler(event, context):
    file_date = datetime.utcnow().strftime('%Y-%m-%d')
    bucket_name = "data-hackathon-smit-muhammad-asad-khalid"
    data_key = f'raw/lambda_yahoofinance/{file_date}.json'
    metadata_key = f'raw/lambda_yahoofinance/metadata{file_date}.json'
    sns_topic_arn = 'arn:aws:sns:us-east-1:376129860326:sns'  # Optional

    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        sp500_table = tables[0]
        symbols = sp500_table['Symbol'].tolist()

        all_data = []

        for symbol in symbols:
            try:
                data = yf.download(
                    tickers=symbol,
                    interval="1d",
                    period="1d",
                    progress=False
                )
                if data.empty:
                    continue

                ohlc = data[['Open', 'High', 'Low', 'Close']].iloc[0]
                all_data.append({
                    'symbol': symbol,
                    'open': round(ohlc['Open'], 2),
                    'high': round(ohlc['High'], 2),
                    'low': round(ohlc['Low'], 2),
                    'close': round(ohlc['Close'], 2)
                })

            except Exception as e:
                print(f"Error for {symbol}: {e}")
                continue

        # Upload main data JSON
        s3.put_object(
            Bucket=bucket_name,
            Key=data_key,
            Body=json.dumps(all_data),
            ContentType="application/json"
        )

        # Upload metadata
        metadata = {
            "source": "yahooFinance",
            "uploaded_at": datetime.utcnow().isoformat(),
            "data_file": data_key
        }
        s3.put_object(
            Bucket=bucket_name,
            Key=metadata_key,
            Body=json.dumps(metadata),
            ContentType="application/json"
        )

        # Optional: Publish SNS message
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

        print(f"Data and metadata uploaded for source {metadata['source']}")

        return {
            'statusCode': 200,
            'body': "Success"
        }

    except Exception as e:
        print("Error:", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


