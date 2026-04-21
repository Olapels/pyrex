import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Initialize the Table Environment in Streaming Mode
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)

# 2. Configure MinIO (S3A) Access
# These settings allow Flink to treat MinIO like a standard S3 bucket
conf = t_env.get_config().get_configuration()
conf.set_string("fs.s3a.endpoint", "http://minio:9000")
conf.set_string("fs.s3a.access.key", "admin")
conf.set_string("fs.s3a.secret.key", "password")
conf.set_string("fs.s3a.path.style.access", "true")  # Required for MinIO
conf.set_string("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# 3. Define the Source: The MinIO Data Lake (Silver Layer)
# 'source.monitor-interval' makes Flink check for new Parquet files every 5 mins
t_env.execute_sql("""
    CREATE TABLE minio_lake (
        company STRING,
        revenue DOUBLE,
        profit DOUBLE,
        filed_date STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = 's3a://sec-lake/parquet/',
        'format' = 'parquet',
        'source.monitor-interval' = '5 min'
    )
""")

# 4. Define the Sink: Postgres (Gold Layer)
# We use 'jdbc' to connect. Note the 'upsert' behavior if a Primary Key is defined.
t_env.execute_sql("""
    CREATE TABLE postgres_analytics (
        company STRING PRIMARY KEY NOT ENFORCED,
        revenue DOUBLE,
        profit DOUBLE,
        filed_date STRING
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/sec_analytics',
        'table-name' = 'summary_report',
        'username' = 'postgres',
        'password' = 'password'
    )
""")

# 5. Execute the Pipeline
# This moves data from the Lake to Postgres continuously.
print("Starting Flink Job: Lake to Postgres...")
t_env.execute_sql("""
    INSERT INTO postgres_analytics
    SELECT company, revenue, profit, filed_date
    FROM minio_lake
""").wait()