import requests
from datetime import datetime
import time
import boto3
import pandas as pd

s3 = boto3.client('s3')

def get_idx(url, path):
    
    headers = {
        'Host': 'www.sec.gov', 
        'Connection': 'keep-alive',
        'Accept-Encoding': 'gzip, deflate',
        'User-Agent': 'Bear Rock bearrockcapital@gmail.com'
         }
    
    try:
        print(f"Retrieving idx file from: {url}")
        indexfile = requests.get(url, headers=headers)
    except Exception as e:
        print(f'Failed to get idx file from url: {url}')
        print(e)
        return False

    print(f"Writing idx file...")
    with open(f'/tmp/{path}', 'wb') as f:
        f.write(indexfile.content)
    
    df = pd.read_fwf(f'/tmp/{path}', colspecs=[(0,61),(62,67)], names=['Company Name', 'Form Type'], skiprows=11)
    print(f'Dataframe: {df}')
    df.to_csv(f"/tmp/{path}.csv", index=False)
    try:
        print(f"Writing to S3. Key: {path}")
        # response = s3.put_object(Body=indexfile.content, Bucket='sec-indexfiles', Key=path)
        response = s3.upload_file(f'/tmp/{path}.csv', Bucket='sec-indexfiles', Key=f'{path}.csv')
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
        print(f"The current time is: {datetime.now()}")
        curr_day = int(time.strftime('%d')) -1
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