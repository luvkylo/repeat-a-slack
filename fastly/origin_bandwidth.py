import json
import time
import calendar
import sys
import hashlib
import re
from datetime import datetime

from service import env
from service import api

from aws_sdk import s3

from db import sql
from db import query


def main():
    now = time.time()
    start = time.gmtime(now)
    # this is the query end time (i.e. this is 2020-10-28T01:00:00Z)
    # start = time.strptime("2021-01-25 00:00:00 +0000", "%Y-%m-%d %H:%M:%S %z")
    startStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print('Script start at', startStr)

    newPyCompleted = time.strptime(time.strftime(
        "%Y-%m-%d %H:00:00", start), "%Y-%m-%d %H:%M:%S")
    newCompleted = time.strftime("%Y-%m-%dT00:00:00Z", start)

    env_var = env.Env()
    queries = query.Queries()
    apis = api.APIrequests()
    S3 = s3.S3()

    print("Connecting to Redshift...")
    # connect to Redshift
    # On QA, this will be connected to the QA reporting Redshift database
    # On PROD, this will be connected to the PROD reporting Redshift database
    redshift = sql.Redshift(
        user=env_var.redshift_user,
        password=env_var.redshift_pw,
        host=env_var.redshift_host,
        database=env_var.redshift_db,
        port=env_var.redshift_port
    )

    print("Getting last timestamp...")

    redshift.execute(queries.getLastCompletedTime(
        job_name='origin_bandwidth'))

    # This is the query start time (i.e. this is 2020-10-28T00:00:00Z)
    # completed = time.strftime(
    #     "%Y-%m-%dT%H:%M:%SZ", time.strptime("2021-07-25 00:00:00 +0000", "%Y-%m-%d %H:%M:%S %z"))
    completed = redshift.returnResult()[0][0].strftime("%Y-%m-%dT%H:%M:%SZ")

    print("Script last completed at", completed)

    pyCompleted = time.strptime(completed, "%Y-%m-%dT%H:%M:%SZ")

    try:
        if pyCompleted < newPyCompleted:

            hashed_id = hashlib.sha256(
                (startStr + 'origin_bandwidth').encode('utf-8')).hexdigest()

            print("Creating log entry...")

            redshift.execute(queries.initiateLog(
                hashed_id=hashed_id, startStr=startStr, job_name='origin_bandwidth'))

            print("Created log entry")
            print("************************************************************")

            print("Query start: " + completed)
            print("Query end: " + newCompleted)

            print("Getting the list of files to process...")

            gmt = time.strptime(time.strftime(
                "%Y-%m-01 00:00:00", pyCompleted), "%Y-%m-%d %H:%M:%S")

            S3.getOriginBandwidthFilelist(
                bucket=env_var.s3_origin_bandwidth_from_bucket_name,
                prefix=env_var.s3_origin_bandwidth_from_bucket_prefix,
                gmt=gmt
            )

            keyList = S3.getKeyList()
            if len(keyList) == 0:
                print("No unprocessed log files detected")
                sys.exit(-1)
            print("File to be process:", keyList)
            print("Downloading file now")

            print("************************************************************")

            insertValues = S3.getOriginBandwidthObject(
                keyList=keyList,
                bucket=env_var.s3_origin_bandwidth_from_bucket_name,
                t=pyCompleted,
                productList=env_var.product_list.split(",")
            )

            if len(insertValues) == 0:
                print("All data processed, no need to run queries")
            else:

                print("Processed", str(len(insertValues)), "line items")

                args_str = b','.join(redshift.cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x)
                                     for x in tuple(insertValues))

                args_str = args_str.decode(
                    "utf-8").replace('::timestamp', '').replace('"', '\\"').replace('“', '\\"').replace('”', '\\"')
                args_str = re.sub('\s+', ' ', args_str)

                print("Writting the results into the database...")

                while len(args_str) > 15000000:
                    index = args_str.find(")", 15000000, 16000000)
                    temp_str = args_str[0: index + 1]
                    args_str = args_str[index + 2:]
                    redshift.execute(
                        "INSERT INTO aws_origin_bandwidth (timestamps, channel_id, account_id, billable_party, distributor, aws_product_name, aws_product_description, usage_quantity, cost) VALUES " + temp_str)
                if len(args_str) > 0:
                    redshift.execute(
                        "INSERT INTO aws_origin_bandwidth (timestamps, channel_id, account_id, billable_party, distributor, aws_product_name, aws_product_description, usage_quantity, cost) VALUES " + args_str)

                print("Data ingested")

            print("************************************************************")

            endStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            completedStr = time.strftime("%Y-%m-%dT00:00:00Z", start)

            redshift.execute(
                queries.completedLog(hashed_id=hashed_id,
                                     endStr=endStr, completedStr=completedStr, job_name='origin_bandwidth')
            )

            print("Log updated...closing connection")

            redshift.closeEverything()
            print("Connection closed")
        else:
            print("No new query")
            redshift.closeEverything()
            print("Connection closed")
            # raise KeyError("No new query")
    except:
        redshift.execute(queries.errorLog(
            hashed_id=hashed_id, error=str(sys.exc_info()[1]), job_name='origin_bandwidth'))

        redshift.closeEverything()

        raise sys.exc_info()[0]


if __name__ == "__main__":
    main()
