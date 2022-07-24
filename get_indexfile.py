import requests
from datetime import datetime
import time
import boto3

s3 = boto3.client('s3')

def get_idx(url, path):

    try:
        print(f"Retrieving idx file from: {url}")
        indexfile = requests.get(url)
    except Exception as e:
        print(f'Failed to get idx file from url: {url}')
        print(e)
        return False

    print(f"Writing idx file...")
    # with open(f'/tmp/{path}', 'wb') as f:
    #     f.write(indexfile.content)
    try:
        response = s3.put_object(Body=indexfile.content, Bucket='sec-indexfiles', Key=path)
    except Exception as e:
        print(f"Failed to put indexfile for: {path}")
        print(e)

    return True

def weekday_check():
    if datetime.weekday(datetime.now()) - 1 < 5:
        return True
    else:
        return False

def lambda_handler(event, context):

    if weekday_check:
        curr_day = time.strftime('%d')
        curr_month = time.strftime('%m')
        curr_quarter = (datetime.now().month - 1) // 3 + 1
        curr_year = time.strftime('%Y')
        path = f"company.{curr_year}{curr_month}{curr_day}.idx"

        url = f"https://www.sec.gov/Archives/edgar/daily-index/{curr_year}/QTR{curr_quarter}/{path}"

        if get_idx(url, path):

        response = {
            'statusCode': 200,
            'body': 'Successful',
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
                }
            } 

        return response
    
    else:

        response = {
        'statusCode': 200,
        'body': 'Aborted... REASON: Not a Weekday!',
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
            }
        }

        return response