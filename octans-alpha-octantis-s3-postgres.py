import json
import pandas as pd
import boto3
from io import StringIO
from boto3.session import Session
import pg8000
import os


def lambda_handler(event, context):
    aws_access_key_id = os.environ.get('aws_access_key_id')
    aws_secret_access_key = os.environ.get('aws_secret_access_key')
    bucket_name_origem = 'tst-lake-octans'

    session = Session(aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    s3 = session.resource('s3')
    bucket = s3.Bucket('tst-lake-octans')
    client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

    for s3_file in bucket.objects.all():
        subdir = s3_file.key.split('/')
        if subdir[0] == 'alpha-octantis-raw' and subdir[1] == 'mongo_livros':
            if ".txt" in s3_file.key:
                # print(s3_file.key)
                client = boto3.client(
                    's3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
                txt_obj = client.get_object(
                    Bucket=bucket_name_origem, Key=s3_file.key)
                body = txt_obj['Body']
                csv_string = body.read().decode('utf-8')
                df = pd.read_csv(StringIO(csv_string), sep="|")
                df['Assunto'] = df['Assunto'].str.replace('[', '', regex=True)
                df['Assunto'] = df['Assunto'].str.replace(']', '', regex=True)
                df['Assunto'] = df['Assunto'].str.replace("'", '', regex=True)
                df['Assunto'] = df['Assunto'].str.replace(
                    ',', ' -', regex=True)

    stringinsertlinha = ""
    for index, row in df.iterrows():
        _id = str(row['_id'])
        titulo = str(row['Titulo'])
        autor = str(row['Autor'])
        ano = str(row['Ano'])
        paginas = str(row['Paginas'])
        assunto = str(row['Assunto'])

        stringinsertlinha = stringinsertlinha + \
            "insert into vialactea.lambdalivros(_id, titulo, autor, ano, paginas, assunto)VALUES("
        stringinsertlinha = stringinsertlinha + "'"+_id+"','" + \
            titulo+"','"+autor+"',"+ano+","+paginas+",'"+assunto+"'"
        stringinsertlinha = stringinsertlinha + ");"

        print(stringinsertlinha)

        conn = pg8000.connect(
            host=os.environ.get('host'),
            database=os.environ.get('database'),
            user=os.environ.get('user'),
            password=os.environ.get('password'),
            port=int(os.environ.get('port')))
        dbcursor = conn.cursor()
        dbcursor.execute(stringinsertlinha)
        conn.commit()
        dbcursor.close()
        stringinsertlinha = ""
        conn.close()
