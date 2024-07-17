import os
import psycopg2
import boto3
from datetime import datetime


# Funções para conexão e download de imagens do banco de dados e S3
def fetch_alerts(batch_size, offset):
    conn = psycopg2.connect(
        host="host_rds",
        database="database",
        user="usuario",
        password="senha"
    )
    cursor = conn.cursor()
    query = """
    SELECT id, categoria, timestamp, camera, tenant, status
    FROM seus_metadados
    LIMIT %s OFFSET %s
    """
    cursor.execute(query, (batch_size, offset))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def generate_s3_path(tenant, timestamp):
    dt = datetime.fromtimestamp(timestamp)
    path = f"harpia-alert/{tenant}/{dt.year}/{dt.month}/{dt.day}/{timestamp}_identifier.jpg"
    return path


def download_images(alerts, local_image_dir):
    s3_client = boto3.client('s3', aws_access_key_id='ACCESS_KEY',
                             aws_secret_access_key='SECRET_KEY', region_name='REGION')
    bucket_name = 's3_bucket'
    if not os.path.exists(local_image_dir):
        os.makedirs(local_image_dir)

    for alert in alerts:
        tenant = alert[4]
        timestamp = alert[2]
        s3_prefix = generate_s3_path(tenant, timestamp)

        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=s3_prefix)
            for obj in response.get('Contents', []):
                s3_key = obj['Key']
                if str(timestamp) in s3_key:
                    local_file_path = os.path.join(
                        local_image_dir, os.path.basename(s3_key))
                    s3_client.download_file(
                        bucket_name, s3_key, local_file_path)
                    print(f"Downloaded {local_file_path}")
        except Exception as e:
            print(f"Could not download image for prefix {s3_prefix}: {e}")
