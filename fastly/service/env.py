import os

from dotenv import load_dotenv
from pathlib import Path


class Env:

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
        self.redshift_host1 = os.getenv('REDSHIFT_HOST1')
        self.redshift_pw1 = os.getenv('REDSHIFT_PASSWORD1')
        self.s3_fastly_from_bucket_name = os.getenv(
            'S3_FASTLY_FROM_BUCKET_NAME')
        self.s3_fastly_logs_from_prefix = os.getenv(
            'S3_FASTLY_LOGS_FROM_PREFIX')
        self.s3_to_bucket_name = os.getenv('S3_TO_BUCKET_NAME')
        self.s3_fastly_logs_to_prefix = os.getenv('S3_FASTLY_LOGS_TO_PREFIX')
        self.s3_origin_bandwidth_from_bucket_name = os.getenv(
            'S3_ORIGIN_BANDWIDTH_FROM_BUCKET_NAME')
        self.s3_origin_bandwidth_from_bucket_prefix = os.getenv(
            'S3_ORIGIN_BANDWIDTH_FROM_BUCKET_PREFIX')
        self.x_frequency_deviceid = os.getenv('X_FREQUENCY_DEVICEID')
        self.x_frequency_auth = os.getenv('X_FREQUENCY_AUTH')
        self.timer_start = os.getenv('TIMER_START')
        self.cores = os.getenv('CORES')
        self.obj_mem = os.getenv('OBJ_MEM')
        self.multicore = os.getenv('MULTICORE_PROC')
        self.fastly_key = os.getenv('FASTLY_KEY')
        self.product_list = os.getenv('PRODUCT_LIST')
