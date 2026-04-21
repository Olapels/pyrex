from kafka import KafkaConsumer
import boto3
import pandas as pd
from io import BytesIO

# Initialize S3 client for MinIO
s3 = boto3.client('s3', endpoint_url='http://localhost:9000', 
                  aws_access_key_id='admin', aws_secret_access_key='password')

consumer = KafkaConsumer('sec_raw', bootstrap_servers='localhost:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for message in consumer:
    data = message.value
    # Convert to Parquet
    df = pd.DataFrame([data])
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    
    # Save to MinIO
    filename = f"parquet/{data['acc_num']}.parquet"
    s3.put_object(Bucket="sec-lake", Key=filename, Body=out_buffer.getvalue())
    print(f"Stored {data['company']} in Lake")