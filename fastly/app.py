import json
import time
import sys

from service import env
from service import pandas

from aws_sdk import s3

from db import sql


def main():
    start = time.time()
    print('Script start at', start)

    env_var = env.Env()
    S3 = s3.S3()

    print("************************************************************")

    # get a list existing logs
    if (env_var.timer_start is None):
        gmt = time.gmtime(start - 7200)
    else:
        gmt = time.gmtime(start - int(env_var.timer_start))
    print("Getting the list of files to process...")
    S3.getlist(
        bucket=env_var.s3_fastly_from_bucket_name,
        prefix=env_var.s3_fastly_logs_from_prefix,
        gmt=gmt
    )

    keyList = S3.getKeyList()
    print("Number of Files:", len(keyList))
    if len(keyList) == 0:
        print("No unprocessed log files detected")
        sys.exit(-1)
    print("Last file process:", keyList[-1])
    print("Got All File Keys...")
    print("Downloading all Files now")
    jsonObj = S3.getDataframeObject(
        keyList=keyList,
        bucket=env_var.s3_fastly_from_bucket_name,
        gmt=gmt
    )

    print("Got All Files")
    print("************************************************************")

    pd = pandas.ETLPandasService()
    pd.etl(jsonObj=jsonObj)
    df = pd.getdf()

    print("************************************************************")
    print("Preparing data for Redshift ingestion...")
    # convert dataframe to numpy array
    np_data = df.to_numpy()
    print(len(np_data))

    print("Connecting to Redshift...")
    # connect to Redshift
    redshift = sql.Redshift(
        user=env_var.redshift_user,
        password=env_var.redshift_pw,
        host=env_var.redshift_host,
        database=env_var.redshift_db,
        port=env_var.redshift_port
    )

    # set query string
    args_str = b','.join(redshift.cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x)
                         for x in tuple(map(tuple, np_data)))

    args_str = args_str.decode("utf-8")
    while len(args_str) > 15000000:
        index = args_str.find(")", 15000000, 16000000)
        temp_str = args_str[0: index + 1]
        args_str = args_str[index + 2:]
        redshift.execute("INSERT INTO fastly_log_aggregated_metadata (timestamps, status, channel_id, distributor, city, country, region, continent, minutes_watched, channel_start, request_size_bytes, request_count, count_720p, count_1080p, between_720p_and_1080p_count, under_720p_count, over_1080p_count) VALUES " + temp_str)
    if len(args_str) > 0:
        redshift.execute("INSERT INTO fastly_log_aggregated_metadata (timestamps, status, channel_id, distributor, city, country, region, continent, minutes_watched, channel_start, request_size_bytes, request_count, count_720p, count_1080p, between_720p_and_1080p_count, under_720p_count, over_1080p_count) VALUES " + args_str)

    redshift.closeEverything()

    print("************************************************************")
    print("Removing processed log files...")
    # remove all processed log files
    S3.moveObjects(
        keyList=keyList,
        bucket=env_var.s3_fastly_from_bucket_name,
        destBucket=env_var.s3_to_bucket_name,
        destFolder=env_var.s3_fastly_logs_to_prefix
    )

    print("************************************************************")
    end = time.time()
    print('Script ends at', end)
    print('Total Elapsed time:', end - start)


if __name__ == '__main__':
    main()
