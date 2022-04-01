import json
import pandas as pd
import boto3
from io import StringIO
from pymongo import MongoClient
from boto3.session import Session
import os


def lambda_handler(event, context):
    aws_access_key_id = os.environ.get('aws_access_key_id')
    aws_secret_access_key = os.environ.get('aws_secret_access_key')
    bucket_name_origem = 'tst-lake-octans'

    session = Session(aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name_origem)
    client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
    s3_resource = boto3.resource(
        's3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    client = MongoClient(os.environ.get('url'))

    db = client.get_database('Biblioteca')
    records1 = db.Livros
    docs1 = records1.find()
    df1 = pd.DataFrame(list(docs1))
    print(df1)
    namefile1 = 'livros.txt'
    csv_buffer1 = StringIO()
    df1.to_csv(csv_buffer1, sep="|", encoding='8859', index=False)
    s3_resource.Object(bucket_name_origem, 'alpha-octantis-raw/mongo_livros/' +
                       namefile1).put(Body=csv_buffer1.getvalue())

    db.close
