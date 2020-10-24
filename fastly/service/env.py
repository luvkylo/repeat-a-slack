import os

from dotenv import load_dotenv
from pathlib import Path


class Env:
    env_path = ''
    if os.path.exists(os.path.join(os.path.dirname(os.getcwd()), '.env')):
        env_path = os.path.join(os.path.dirname(os.getcwd()), '.env')
        load_dotenv(dotenv_path=env_path)

    # get all redshift credentials and s3 bucket name and prefix
    def __init__(self):
        self.redshift_user = os.getenv('REDSHIFT_USER')
        self.redshift_db = os.getenv('REDSHIFT_DATABASE')
        self.redshift_pw = os.getenv('REDSHIFT_PASSWORD')
        self.redshift_host = os.getenv('REDSHIFT_HOST')
        self.redshift_port = os.getenv('REDSHIFT_PORT')
        self.s3_fastly_from_bucket_name = os.getenv(
            'S3_FASTLY_FROM_BUCKET_NAME')
        self.s3_fastly_logs_from_prefix = os.getenv(
            'S3_FASTLY_LOGS_FROM_PREFIX')
        print(self.s3_fastly_logs_from_prefix)
